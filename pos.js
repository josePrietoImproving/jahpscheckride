const fs = require('fs');

function readConfigFile(fileName) {
    const data = fs.readFileSync(fileName, 'utf8').toString().split("\n");
    return data.reduce((config, line) => {
        const [key, value] = line.split("=");
        if (key && value) {
            config[key] = value;
        }
        return config;
    }, {})
}

const Kafka = require("node-rdkafka");
const config = readConfigFile("client.properties");
config["group.id"] = "node-group";
/* 

- send information from john doe (produce)
- listen for topic "result client"


*/
const consumer = new Kafka.KafkaConsumer(config, {"auto.offset.reset": "earliest" });
consumer.connect();
consumer.on("ready", () => {
    consumer.subscribe(["topic_0",""]);
    consumer.consume();
}).on("data", (message) => {
    console.log("Consumed message", message);
});