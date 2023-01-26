const { Kafka } = require("kafkajs");

const topic_name = process.argv[2] || "Logs2";
const partition = process.argv[3] || 0 ;

createProducer();

async function createProducer() {
  try {
    // Admin Stuff..
    const kafka = new Kafka({
        clientId: "kafka_pub_sub_client",
        brokers: ["192.168.2.33:9092"]
    });

    const producer = kafka.producer();
    console.log("Producer'a bağlanılıyor..");
    await producer.connect();
    console.log("Producer'a bağlantı başarılı");

    const message_result = await producer.send({
        topic : "raw_video_topic",
        messages: [
            {
                value: "Bu bir test log mesajıdır..",
                partition: partition 
            }
        ]
    })

    console.log("Gönderim işlemi başarılıdır", JSON.stringify(message_result));

    await producer.disconnect();

    
  } catch (error) {
    console.log("Bir Hata Oluştu", error);
  } finally {
    process.exit(0);
  }
}