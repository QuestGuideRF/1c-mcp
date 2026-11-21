import asyncio
import json
import logging
from typing import Dict ,Any ,Optional
from contextlib import asynccontextmanager
from urllib .parse import urlencode ,parse_qs
from fastapi import FastAPI ,Request ,Response ,HTTPException ,Form
from fastapi .responses import StreamingResponse ,HTMLResponse ,RedirectResponse ,JSONResponse
from fastapi .middleware .cors import CORSMiddleware
import uvicorn
import httpx
from mcp .server .sse import SseServerTransport
from mcp .server .streamable_http_manager import StreamableHTTPSessionManager
from mcp .server .models import InitializationOptions
from starlette .types import Scope ,Receive ,Send
from starlette .middleware .base import BaseHTTPMiddleware
from .mcp_server import MCPProxy ,current_onec_credentials
from .config import Config
from .auth import OAuth2Service ,OAuth2Store
logger =logging .getLogger (__name__ )
class OAuth2BearerMiddleware (BaseHTTPMiddleware ):
	def __init__ (self ,app ,oauth2_service :Optional [OAuth2Service ],auth_mode :str ):
		super ().__init__ (app )
		self .oauth2_service =oauth2_service
		self .auth_mode =auth_mode
		self .protected_paths =["/mcp/","/sse"]
	async def dispatch (self ,request :Request ,call_next ):
		if self .auth_mode !="oauth2":
			return await call_next (request )
		path =request .url .path
		is_protected =any (path .startswith (protected )for protected in self .protected_paths )
		if not is_protected :
			return await call_next (request )
		auth_header =request .headers .get ("Authorization","")
		if not auth_header .startswith ("Bearer "):
			return JSONResponse (
			status_code =401 ,
			content ={"error":"invalid_token"},
			headers ={"WWW-Authenticate":'Ошибка носителя="invalid_token"'}
			)
		token =auth_header [7 :]
		creds =None
		if token .startswith ("simple_"):
			try :
				import base64
				creds_string =base64 .b64decode (token [7 :]).decode ()
				username ,password =creds_string .split (":",1 )
				creds =(username ,password )
				logger .debug (f"Простой токен валидирован для пользователя: {username }")
			except Exception as e :
				logger .warning (f"Ошибка декодирования простого токена: {e }")
				creds =None
		if not creds :
			creds =self .oauth2_service .validate_access_token (token )
		if not creds :
			return JSONResponse (
			status_code =401 ,
			content ={"error":"invalid_token"},
			headers ={"WWW-Authenticate":'Ошибка носителя="invalid_token"'}
			)
		login ,password =creds
		current_onec_credentials .set ((login ,password ))
		response =await call_next (request )
		return response
class MCPHttpServer :
	def __init__ (self ,config :Config ):
		self .config =config
		self .mcp_proxy =MCPProxy (config )
		self .streamable_session_manager =StreamableHTTPSessionManager (self .mcp_proxy .server )
		self .oauth2_store :Optional [OAuth2Store ]=None
		self .oauth2_service :Optional [OAuth2Service ]=None
		if config .auth_mode =="oauth2":
			self .oauth2_store =OAuth2Store ()
			self .oauth2_service =OAuth2Service (
			self .oauth2_store ,
			code_ttl =config .oauth2_code_ttl ,
			access_ttl =config .oauth2_access_ttl ,
			refresh_ttl =config .oauth2_refresh_ttl
			)
			logger .info ("Авторизация OAuth2 включена")
		self .app =FastAPI (
		title ="1С МКП Прокси",
		description ="MCP-прокси для взаимодействия с 1С",
		version =config .server_version ,
		lifespan =self ._lifespan
		)
		self .app .add_middleware (
		CORSMiddleware ,
		allow_origins =config .cors_origins ,
		allow_credentials =True ,
		allow_methods =["*"],
		allow_headers =["*"],
		)
		self .app .add_middleware (
		OAuth2BearerMiddleware ,
		oauth2_service =self .oauth2_service ,
		auth_mode =config .auth_mode
		)
		self ._mount_transports ()
		self ._register_routes ()
	@asynccontextmanager
	async def _lifespan (self ,app :FastAPI ):
		logger .debug ("Запуск HTTP-сервера MCP")
		if self .oauth2_store :
			await self .oauth2_store .start_cleanup_task (interval =60 )
		async with self .streamable_session_manager .run ():
			yield
		if self .oauth2_store :
			await self .oauth2_store .stop_cleanup_task ()
		logger .debug ("Остановка HTTP-сервера MCP")
	def _create_sse_asgi (self ):
		sse_transport =SseServerTransport ("/messages/")
		async def asgi (scope :Scope ,receive :Receive ,send :Send )->None :
			if scope ["type"]=="lifespan":
				while True :
					message =await receive ()
					if message ["type"]=="lifespan.startup":
						await send ({"type":"lifespan.startup.complete"})
					elif message ["type"]=="lifespan.shutdown":
						await send ({"type":"lifespan.shutdown.complete"})
						return
			if scope ["type"]!="http":
				return
			path =scope ["path"]
			if path .startswith ("/sse"):
				path =path [4 :]
			if not path :
				path ="/"
			if path =="/messages":
				path ="/messages/"
			method =scope ["method"]
			if method =="GET"and path =="/":
				logger .debug ("Новое SSE подключение")
				try :
					async with sse_transport .connect_sse (scope ,receive ,send )as streams :
						await self .mcp_proxy .server .run (
						streams [0 ],
						streams [1 ],
						self .mcp_proxy .get_initialization_options ()
						)
				except Exception as e :
					logger .error (f"Ошибка в SSE обработчике: {e }")
					raise
				finally :
					logger .debug ("SSE подключение закрыто")
			elif method =="POST"and path .startswith ("/messages"):
				adjusted_scope =dict (scope )
				adjusted_scope ["path"]=path
				if "raw_path"in adjusted_scope :
					adjusted_scope ["raw_path"]=path .encode ("utf-8")
				await sse_transport .handle_post_message (adjusted_scope ,receive ,send )
			else :
				await send ({
				"type":"http.response.start",
				"status":404 ,
				"headers":[[b"content-type",b"text/plain"]],
				})
				await send ({
				"type":"http.response.body",
				"body":b"Not Found",
				})
		return asgi
	def _create_streamable_http_asgi (self ):
		async def asgi (scope :Scope ,receive :Receive ,send :Send )->None :
			logger .debug ("Новое Streamable HTTP подключение")
			try :
				await self .streamable_session_manager .handle_request (scope ,receive ,send )
			except Exception as e :
				logger .error (f"Ошибка в Streamable HTTP обработчике: {e }")
				raise
			finally :
				logger .debug ("Streamable HTTP подключение закрыто")
		return asgi
	def _mount_transports (self ):
		sse_app =self ._create_sse_asgi ()
		self .app .mount ("/sse",sse_app )
		streamable_app =self ._create_streamable_http_asgi ()
		self .app .mount ("/mcp/",streamable_app )
	def _register_routes (self ):
		@self .app .get ("/")
		async def root ():
			endpoints ={
			"info":"/info",
			"health":"/health",
			"sse":"/sse",
			"streamable_http":"/mcp/"
			}
			if self .config .auth_mode =="oauth2":
				endpoints ["oauth2"]={
				"well_known_prm":"/.well-known/oauth-protected-resource",
				"well_known_as":"/.well-known/oauth-authorization-server",
				"register":"/register",
				"authorize":"/authorize",
				"token":"/token"
				}
			return {
			"message":"Прокси-сервер 1С МКП",
			"endpoints":endpoints
			}
		@self .app .get ("/info")
		async def info ():
			return {
			"name":self .config .server_name ,
			"version":self .config .server_version ,
			"description":"MCP-прокси для взаимодействия с 1С",
			"endpoints":{
			"sse":"/sse",
			"messages":"/sse/messages/",
			"streamable_http":"/mcp/",
			"health":"/health",
			"info":"/info"
			},
			"transports":{
			"sse":{
			"endpoint":"/sse",
			"messages":"/sse/messages/"
			},
			"streamable_http":{
			"endpoint":"/mcp/"
			}
			}
			}
		@self .app .get ("/health")
		async def health ():
			try :
				if hasattr (self .mcp_proxy ,'onec_client')and self .mcp_proxy .onec_client :
					await self .mcp_proxy .onec_client .check_health ()
					result ={"status":"healthy","onec_connection":"ok"}
				else :
					result ={"status":"starting","onec_connection":"not_initialized"}
				result ["auth"]={"mode":self .config .auth_mode }
				return result
			except Exception as e :
				logger .error (f"Ошибка проверки здоровья: {e }")
				return {
				"status":"unhealthy",
				"onec_connection":"error",
				"error_details":str (e ),
				"auth":{"mode":self .config .auth_mode }
				}
		if self .config .auth_mode =="oauth2":
			self ._register_oauth2_routes ()
	def _register_oauth2_routes (self ):
		@self .app .get ("/.well-known/oauth-protected-resource")
		async def well_known_prm (request :Request ):
			if self .config .public_url :
				public_url =self .config .public_url
			else :
				scheme =request .url .scheme
				netloc =request .headers .get ("host",f"{request .client .host }:{request .url .port }")
				public_url =f"{scheme }://{netloc }"
			return self .oauth2_service .generate_prm_document (public_url )
		@self .app .get ("/.well-known/oauth-authorization-server")
		async def well_known_as_metadata (request :Request ):
			if self .config .public_url :
				base_url =self .config .public_url
			else :
				scheme =request .url .scheme
				netloc =request .headers .get ("host",f"{request .client .host }:{request .url .port }")
				base_url =f"{scheme }://{netloc }"
			return {
			"issuer":base_url ,
			"authorization_endpoint":f"{base_url }/authorize",
			"token_endpoint":f"{base_url }/token",
			"registration_endpoint":f"{base_url }/register",
			"grant_types_supported":[
			"authorization_code",
			"refresh_token",
			"password"
			],
			"response_types_supported":["code"],
			"code_challenge_methods_supported":["S256"],
			"token_endpoint_auth_methods_supported":["none"],
			"revocation_endpoint_auth_methods_supported":["none"]
			}
		@self .app .post ("/register")
		async def register_client (request :Request ):
			try :
				body =await request .json ()
				logger .debug (f"Client registration request: {body }")
			except :
				body ={}
			if self .config .public_url :
				base_url =self .config .public_url
			else :
				scheme =request .url .scheme
				netloc =request .headers .get ("host",f"{request .client .host }:{request .url .port }")
				base_url =f"{scheme }://{netloc }"
			client_data ={
			"client_id":"mcp-public-client",
			"client_secret":"",
			"client_id_issued_at":1640000000 ,
			"grant_types":["authorization_code","refresh_token","password"],
			"response_types":["code"],
			"redirect_uris":[
			f"{base_url }/callback",
			"http://localhost/callback",
			"http://127.0.0.1/callback"
			],
			"token_endpoint_auth_method":"none",
			"application_type":"web"
			}
			if "redirect_uris"in body :
				for uri in body .get ("redirect_uris",[]):
					if uri not in client_data ["redirect_uris"]:
						client_data ["redirect_uris"].append (uri )
			logger .info (f"Client registration: вернули фиксированный client_id='mcp-public-client'")
			return client_data
		@self .app .get ("/authorize")
		async def authorize_get (
		request :Request ,
		response_type :str =None ,
		client_id :str =None ,
		redirect_uri :str =None ,
		state :str =None ,
		code_challenge :str =None ,
		code_challenge_method :str =None
		):
			if not all ([response_type ,client_id ,redirect_uri ,code_challenge ,code_challenge_method ]):
				return HTMLResponse (
				content ="<html><body><h1>Ошибка</h1><p>Отсутствуют обязательные параметры OAuth2</p></body></html>",
				status_code =400
				)
			if response_type !="code":
				return HTMLResponse (
				content ="<html><body><h1>Ошибка</h1><p>Поддерживается только response_type=code</p></body></html>",
				status_code =400
				)
			if code_challenge_method !="S256":
				return HTMLResponse (
				content ="<html><body><h1>Ошибка</h1><p>Поддерживается только code_challenge_method=S256</p></body></html>",
				status_code =400
				)
			query_params =urlencode ({
			"redirect_uri":redirect_uri ,
			"state":state or "",
			"code_challenge":code_challenge
			})
			html_content =f
			return HTMLResponse (content =html_content )
		@self .app .post ("/authorize")
		async def authorize_post (
		request :Request ,
		username :str =Form (...),
		password :str =Form (...),
		redirect_uri :str =None ,
		state :str =None ,
		code_challenge :str =None
		):
			if not all ([redirect_uri ,code_challenge ]):
				return HTMLResponse (
				content ="<html><body><h1>Ошибка</h1><p>Отсутствуют обязательные параметры</p></body></html>",
				status_code =400
				)
			try :
				async with httpx .AsyncClient (timeout =10.0 )as client :
					health_url =f"{self .config .onec_url }/hs/{self .config .onec_service_root }/health"
					response =await client .get (
					health_url ,
					auth =httpx .BasicAuth (username ,password )
					)
					if response .status_code ==401 :
						error_html =f
						return HTMLResponse (content =error_html ,status_code =401 )
					if response .status_code ==403 :
						error_html =f
						return HTMLResponse (content =error_html ,status_code =403 )
					if response .status_code !=200 :
						logger .warning (f"1С вернула статус {response .status_code } при проверке креденшилов пользователя {username }")
						return HTMLResponse (
						content =f"<html><body><h1>Ошибка</h1><p>1С вернула неожиданный статус {response .status_code }</p></body></html>",
						status_code =502
						)
			except Exception as e :
				logger .error (f"Ошибка проверки креденшилов 1С: {e }")
				return HTMLResponse (
				content =f"<html><body><h1>Ошибка</h1><p>Не удалось подключиться к 1С: {e }</p></body></html>",
				status_code =503
				)
			code =self .oauth2_service .generate_authorization_code (
			login =username ,
			password =password ,
			redirect_uri =redirect_uri ,
			code_challenge =code_challenge
			)
			params ={"code":code }
			if state :
				params ["state"]=state
			redirect_url =f"{redirect_uri }?{urlencode (params )}"
			logger .info (f"Authorization code выдан для пользователя {username }, redirect: {redirect_uri }")
			return RedirectResponse (url =redirect_url ,status_code =302 )
		@self .app .post ("/token")
		async def token_endpoint (
		request :Request ,
		grant_type :str =Form (...),
		code :str =Form (None ),
		redirect_uri :str =Form (None ),
		code_verifier :str =Form (None ),
		refresh_token :str =Form (None ),
		username :str =Form (None ),
		password :str =Form (None )
		):
			if grant_type =="password":
				if username is None :
					return JSONResponse (
					status_code =400 ,
					content ={"error":"invalid_request","error_description":"Отсутствует имя пользователя"}
					)
				try :
					async with httpx .AsyncClient (timeout =10.0 )as client :
						health_url =f"{self .config .onec_url }/hs/{self .config .onec_service_root }/health"
						response =await client .get (
						health_url ,
						auth =httpx .BasicAuth (username ,password or "")
						)
						if response .status_code ==401 :
							return JSONResponse (
							status_code =400 ,
							content ={"error":"invalid_grant","error_description":"Неверное имя пользователя или пароль"}
							)
						if response .status_code ==403 :
							return JSONResponse (
							status_code =403 ,
							content ={"error":"insufficient_scope","error_description":f"User '{username }' does not have permissions for MCP HTTP service. Contact 1C administrator to assign the required role."}
							)
						if response .status_code !=200 :
							logger .warning (f"1C returned status {response .status_code } during credential check for user {username }")
							return JSONResponse (
							status_code =502 ,
							content ={"error":"server_error","error_description":f"1C returned unexpected status {response .status_code }"}
							)
				except Exception as e :
					logger .error (f"Ошибка проверки креденшилов 1С для password grant: {e }")
					return JSONResponse (
					status_code =503 ,
					content ={"error":"server_error","error_description":"Не удалось проверить учетные данные"}
					)
				import base64
				creds_string =f"{username }:{password }"
				simple_token ="simple_"+base64 .b64encode (creds_string .encode ()).decode ()
				logger .info (f"Password grant выдан для пользователя {username }")
				return {
				"access_token":simple_token ,
				"token_type":"Bearer",
				"expires_in":86400 ,
				"scope":"mcp"
				}
			if grant_type =="authorization_code":
				if not all ([code ,redirect_uri ,code_verifier ]):
					return JSONResponse (
					status_code =400 ,
					content ={"error":"invalid_request","error_description":"Отсутствуют необходимые параметры"}
					)
				result =self .oauth2_service .exchange_code_for_tokens (code ,redirect_uri ,code_verifier )
				if not result :
					return JSONResponse (
					status_code =400 ,
					content ={"error":"invalid_grant","error_description":"Неверный или просроченный код авторизации"}
					)
				access_token ,token_type ,expires_in ,refresh =result
				return {
				"access_token":access_token ,
				"token_type":token_type ,
				"expires_in":expires_in ,
				"refresh_token":refresh ,
				"scope":"mcp"
				}
			elif grant_type =="refresh_token":
				if not refresh_token :
					return JSONResponse (
					status_code =400 ,
					content ={"error":"invalid_request","error_description":"Отсутствует обновить_токен"}
					)
				result =self .oauth2_service .refresh_tokens (refresh_token )
				if not result :
					return JSONResponse (
					status_code =400 ,
					content ={"error":"invalid_grant","error_description":"Неверный или просроченный токен обновления."}
					)
				access_token ,token_type ,expires_in ,new_refresh =result
				return {
				"access_token":access_token ,
				"token_type":token_type ,
				"expires_in":expires_in ,
				"refresh_token":new_refresh ,
				"scope":"mcp"
				}
			else :
				return JSONResponse (
				status_code =400 ,
				content ={"error":"unsupported_grant_type","error_description":f"Grant type '{grant_type }' not supported"}
				)
	async def start (self ):
		config =uvicorn .Config (
		app =self .app ,
		host =self .config .host ,
		port =self .config .port ,
		log_level =self .config .log_level .lower (),
		access_log =True
		)
		server =uvicorn .Server (config )
		logger .debug (f"Запуск HTTP-сервера на {self .config .host }:{self .config .port }")
		await server .serve ()
async def run_http_server (config :Config ):
	server =MCPHttpServer (config )
	await server .start ()