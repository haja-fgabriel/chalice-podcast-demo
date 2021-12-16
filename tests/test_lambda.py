from chalicelib.aws.awslambda import *

import json


def test_lambda_client():
    client = lambda_client()
    assert client


def test_invoke_lambda():
    response = invoke_lambda("HelloWorld")
    assert isinstance(response, dict)
    assert response["StatusCode"] == 200


def test_invoke_lambda_async():
    response = invoke_lambda("HelloWorld", invoke_async=True)
    assert isinstance(response, dict)
    assert response["StatusCode"] == 202


def test_invoke_lambda_with_payload():
    response = invoke_lambda("HelloWorld", payload=b'{ "items": [1,2,3] }')
    assert isinstance(response, dict)
    assert response["StatusCode"] == 200
    payload = json.load(response["Payload"])
    body = json.loads(payload["body"])
    assert body["items"] == [1, 2, 3]
