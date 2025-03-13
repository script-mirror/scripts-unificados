import uvicorn
from sys import path
import os

from fastapi import FastAPI, Depends
from app.routers import rodadas_controller, ons_controller, bbce_controller, decks_controller, back_viz_controller
from app.utils.cache import cache
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(
    title="API Trading - Middle",
    version="2.0.0",
    docs_url="/api/v2/docs",
    redoc_url="/api/v2/redoc",
    openapi_url="/api/v2/openapi.json"
)

# CORS
origins = [
    "http://localhost:5173",
    "http://localhost:5000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rodadas_controller.router, prefix="/api/v2")
app.include_router(ons_controller.router, prefix="/api/v2")
app.include_router(bbce_controller.router, prefix="/api/v2")
app.include_router(decks_controller.router, prefix="/api/v2")
app.include_router(back_viz_controller.router, prefix="/api/v2")

# app.include_router(testes_controller.router, prefix='/api')

@app.on_event("shutdown")
def shutdown():
    cache.close()
    
def main() -> None:
    uvicorn.run(app, port=8000, host='0.0.0.0')
        
    
if __name__ == "__main__":
    main()