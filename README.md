# Bonusbox toy

## About
Using Kafka Streams to aggregate purchases into meaningful data, to provide personalized discounts to customers.

## Braindump
![This probably only makes sense to the person who jotted it down ; )](/notes/braindump_2023-11-04.png)

## Developing
```sh
git clone "git@github.com:swapsCAPS/bonusbox-kstreams.git"
cd bonusbox-kstreams
docker-compose -up -d
open http://localhost:9080
./gradlew schemas:avro-schemas:generateAvroJava
./gradlew apps:producer:run
```

## TODO
- [ ] Don't crash if topics already exist
- [ ] Store streams output in DB
- [ ] Apply discounts based on same season previous purchases
- [ ] Write checkout service
- [ ] 
- [ ] 
- [ ] 
- [ ] 
