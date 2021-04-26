import uvicorn

from flowsaber.server.app import app


def test_app():
    uvicorn.run(app=app)


if __name__ == '__main__':
    test_app()
