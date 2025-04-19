// resolvers.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
// Charger les fichiers proto pour les films et les séries TV
const movieProtoPath = 'movie.proto';
const tvShowProtoPath = 'tvShow.proto';
const { Kafka } = require('kafkajs');
const movieProtoDefinition = protoLoader.loadSync(movieProtoPath, {
keepCase: true,
longs: String,
enums: String,
defaults: true,
oneofs: true,
});
const kafka = new Kafka({
    clientId: 'api-gateway',
    brokers: ['localhost:9092'],
  });
  const producer = kafka.producer();
  const sendMessage = async (topic, message) => {
    await producer.connect();
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(message) }],
    });
    await producer.disconnect();
  };
  
const tvShowProtoDefinition = protoLoader.loadSync(tvShowProtoPath, {
keepCase: true,
longs: String,
enums: String,
defaults: true,
oneofs: true,
});
const movieProto = grpc.loadPackageDefinition(movieProtoDefinition).movie;
const tvShowProto = grpc.loadPackageDefinition(tvShowProtoDefinition).tvShow;
// Définir les résolveurs pour les requêtes GraphQL
const resolvers = {
Query: {
movie: (_, { id }) => {
// Effectuer un appel gRPC au microservice de films
const client = new movieProto.MovieService('localhost:50051',
grpc.credentials.createInsecure());
return new Promise((resolve, reject) => {
client.getMovie({ movieId: id }, (err, response) => {
if (err) {
reject(err);
} else {
resolve(response.movie);
}
});
});
},
movies: () => {
// Effectuer un appel gRPC au microservice de films
const client = new movieProto.MovieService('localhost:50051',
grpc.credentials.createInsecure());
return new Promise((resolve, reject) => {
client.searchMovies({}, (err, response) => {
if (err) {
reject(err);
} else {
resolve(response.movies);
}
});
});
},
tvShow: (_, { id }) => {
    // Effectuer un appel gRPC au microservice de séries TV
    const client = new tvShowProto.TVShowService('localhost:50052',
    grpc.credentials.createInsecure());
    return new Promise((resolve, reject) => {
    client.getTvshow({ tvShowId: id }, (err, response) => {
    if (err) {
    reject(err);
    } else {
    resolve(response.tv_show);
    }
    });
    });
    },
    tvShows: () => {
    // Effectuer un appel gRPC au microservice de séries TV
    const client = new tvShowProto.TVShowService('localhost:50052',
    grpc.credentials.createInsecure());
    return new Promise((resolve, reject) => {
    client.searchTvshows({}, (err, response) => {
    if (err) {
    reject(err);
    } else {
    resolve(response.tv_shows);
    }
    });
    });
    
    },
    Mutation: {
        createMovie: async (_, movieData) => {
          await sendMessage('movies_topic', movieData);
          return movieData;
        },
        createTVShow: async (_, tvShowData) => {
          await sendMessage('tvshows_topic', tvShowData);
          return tvShowData;
        },
    },
    },
    };
    module.exports = resolvers;