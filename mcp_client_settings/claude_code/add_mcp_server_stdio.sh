claude mcp add --transport stdio 1c-md \
  --env PYTHONPATH=D:/rep/1c_mcp \
  --env PYTHONIOENCODING=utf-8 \
  --env MCP_ONEC_URL=http://localhost/my_base \
  --env MCP_ONEC_USERNAME=<пользователь 1С> \
  --env MCP_ONEC_PASSWORD=<пароль пользователя 1С> \
  -- d:/rep/1c_mcp/venv/Scripts/python.exe -m src.py_server