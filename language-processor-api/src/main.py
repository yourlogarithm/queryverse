import uvicorn
from fastapi import FastAPI, Request, Response
from sentence_transformers import SentenceTransformer
from vector_pb2 import VectorResponse

EMBEDDER = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

app = FastAPI()


@app.get("/")
def root():
    return {"status": "OK"}


@app.post("/embed")
async def embed(request: Request):
    text = await request.body()
    vector = EMBEDDER.encode(text.decode("utf-8"))
    binary_response = VectorResponse(value=vector).SerializeToString()
    return Response(content=binary_response, media_type="application/octet-stream")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        ssl_certfile="certificates/certificate.pem",
        ssl_keyfile="certificates/private-key.pem",
    )
