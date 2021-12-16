import boto3


def lambda_client():
    return boto3.Session().client("lambda")


def invoke_lambda(lambda_name: str, payload: bytes = b"", invoke_async: bool = False):
    return lambda_client().invoke(
        FunctionName=lambda_name,
        Payload=payload,
        InvocationType="Event" if invoke_async else "RequestResponse",
    )
