docker network create job_network

docker build -t cover_generator ./cover_generator
docker build -t gateway ./gateway
docker build -t vacancies ./vacancies
docker build -t tests ./tests

docker run -d --name rabbitmq --network job_network -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=strongpassword rabbitmq:4-management
TIMEOUT /T 10 /NOBREAK

docker exec rabbitmq rabbitmqctl add_user gateway gateway
docker exec rabbitmq rabbitmqctl add_user vacancies vacancies
docker exec rabbitmq rabbitmqctl add_user cover_generator cover_generator
docker exec rabbitmq rabbitmqctl add_user tests tests

docker exec rabbitmq rabbitmqctl set_permissions -p / gateway ".*" ".*" ".*"
docker exec rabbitmq rabbitmqctl set_permissions -p / vacancies ".*" ".*" ".*"
docker exec rabbitmq rabbitmqctl set_permissions -p / cover_generator ".*" ".*" ".*"
docker exec rabbitmq rabbitmqctl set_permissions -p / tests ".*" ".*" ".*"

docker run -d --name gateway  --network job_network -p 8000:8000 gateway
docker run -d --name vacancies  --network job_network vacancies
docker run -d --name cover_generator  --network job_network cover_generator
docker run -d --name tests  --network job_network tests