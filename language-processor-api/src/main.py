import uvicorn
from fastapi import FastAPI, Request, Response
# from pydantic import PROTOBUF, BaseModel
# from sentence_transformers import SentenceTransformer
from transformers import AutoModel
from vector_pb2 import VectorResponse

MODEL = AutoModel.from_pretrained('jinaai/jina-embeddings-v2-base-en', trust_remote_code=True)

app = FastAPI()

# class SimilarityQuery(BaseModel):


@app.get("/")
def root():
    return {"status": "OK"}


@app.get("/embedding")
async def embedding(request: Request):
    text = await request.body()
    text = text.decode("utf-8")
    vector = MODEL.encode(text, max_length=8192)
    binary_response = VectorResponse(value=vector).SerializeToString()
    return Response(content=binary_response, media_type="application/octet-stream")


# @app.get("/similarity")
# async def similarity(request: PROTOBUF()):
#     pass


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        ssl_certfile="certificates/certificate.pem",
        ssl_keyfile="certificates/privatekey.pem",
    )
