const fs = require("fs");
const avro = require("avsc");
const type = avro.Type.forSchema(
  {
    "connect.name": "ksql.users",
    fields: [
      {
        name: "registertime",
        type: "long",
      },
      {
        name: "userid",
        type: "string",
      },
      {
        name: "regionid",
        type: "string",
      },
      {
        name: "gender",
        type: "string",
      },
    ],
    name: "users",
    namespace: "ksql",
    type: "record",
  }
);
function readConfigFile(fileName) {
  const data = fs.readFileSync(fileName, "utf8").toString().split("\n");
  return data.reduce((config, line) => {
    const [key, value] = line.split("=");
    if (key && value) {
      config[key] = value;
    }
    return config;
  }, {});
}
function generateRandom(min = 300, max = 900) {
  let difference = max - min;
  let rand = Math.random();
  rand = Math.floor(rand * difference);
  rand = rand + min;

  return rand;
}

function mockBureauCall(user) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve({
        userid: user.userid,
        score: generateRandom(),
      });
      reject({});
    }, 2000);
  });
}

/* Main Logic */
const Kafka = require("node-rdkafka");
const config = readConfigFile("client.properties");
config["group.id"] = "node-group";

const consumer = new Kafka.KafkaConsumer(config, {
  "auto.offset.reset": "earliest",
});
const producer = new Kafka.Producer(readConfigFile("client.properties"));
producer.connect();
producer.on("ready", () => {
  consumer.connect();
  consumer
    .on("ready", () => {
      consumer.subscribe(["new-client"]);
      consumer.consume();
        //consumer and producer ready, listen to new client topic

    })
    .on("data", (message) => {

      var clientData = JSON.parse(message.value);
      mockBureauCall(clientData).then((bureauResult) => {
        setTimeout(() => {
          //got results, send them to the according topic for persistence/further processing
          producer.produce(
            "processed_clients",
            -1,
            Buffer.from(JSON.stringify(bureauResult)),
            Buffer.from(bureauResult.userid)
          );
          console.log('sent: ', bureauResult);
        }, 3000);
      });
    });
});
