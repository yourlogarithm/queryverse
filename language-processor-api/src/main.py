import uvicorn
from fastapi import FastAPI, Request, Response
from sentence_transformers import SentenceTransformer
from vector_pb2 import VectorResponse

MODEL = SentenceTransformer('/app/gte-small')

app = FastAPI()

@app.get("/")
def root():
    return {"status": "OK"}


@app.get("/embedding")
async def embedding(request: Request):
    text = await request.body()
    text = text.decode("utf-8")
    vector = MODEL.encode(text)
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
    )
