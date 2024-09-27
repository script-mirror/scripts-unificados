from fastapi import Request, HTTPException
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from dotenv import load_dotenv
from typing import Optional
import asyncio
import os
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

SECRET_KEY= os.getenv('FLASK_SECRET_KEY')


SESSION_COOKIE_NAME = 'session'

def decode_flask_cookie(cookie_value: str):
    serializer = URLSafeTimedSerializer(SECRET_KEY)
    try:
        session_data = serializer.loads(cookie_value)
        return session_data
    except Exception:
        raise HTTPException(status_code=403, detail="Cookie invalido")

def auth_dependency(request: Request):
    session_cookie = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_cookie:
        raise HTTPException(status_code=401, detail="Cookie nao encontrado")
    
    session_data = decode_flask_cookie(session_cookie)
    
    if "user_id" not in session_data:
        raise HTTPException(status_code=403, detail="Usuario nao autenticado")
    
    return session_data


    
if __name__ == "__main__":
    asyncio.run(decode_flask_cookie(""))
    


