import docker

client = docker.from_env()

container = client.containers.run(
    "postgres",
    name="my-postgres",
    detach=True,
    environment={"POSTGRES_PASSWORD": "mysecretpassword"},
    ports={"5432/tcp": 5432},
    volumes={os.path.expanduser("~/Desktop/pgdata"): {"bind": "/var/lib/postgresql/data", "mode": "rw"}},
)