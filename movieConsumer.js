const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');
const Movie = require('./models/Movie');

mongoose.connect('mongodb://localhost:27017/moviesDB', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
}).then(() => console.log('✅ Connecté à MongoDB pour films'));

const kafka = new Kafka({ clientId: 'movie-service', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'movie-group' });

const consumeMessages = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'movies_topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      const movie = new Movie(data);
      await movie.save();
      console.log('🎬 Nouveau film enregistré via Kafka');
    }
  });
};

consumeMessages();
