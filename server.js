//------------------------------------------------------------------------------
// cloud-manager-daemon.js
// This daemon is responsible for reading all of the sensor values from the
// Intel Iot Gateway and storing them in the cloud
//
// Currently, this daemon supports four cloud providers:
//     1. Microsoft Azure
//     2. IBM BlueMix
//
// The daemon retrieves all new data from the database every 60 seconds
// and retrieves the associations between sensors and cloud providers
// and store the data in blocks on the cloud providers that are configured
// as destinations.

// Load the application specific configurations
var config = require("./config.json");

// Require the MongoDB libraries and connect to the database
var mongoose = require('mongoose');

mongoose.connect(config.mongodb.testHost);
var db = mongoose.connection;

// Report database errors to the console
db.on('error', console.error.bind(console, 'connection error:'));

// Log when a connection is established to the MongoDB server
db.once('open', function (callback) {
  console.info("Connection to MongoDB successful");
});

// Lodash is a functional library for manipulating data structures
var _ = require("lodash");

var Azure = require('intel-commercial-iot-microsoft-azure-storage');
var IBM = require('intel-commercial-iot-ibm-bluemix-storage');

var DataModel = require('intel-commerical-edge-network-database-models').DataModel;
var SensorCloudModel = require('intel-commerical-edge-network-database-models').SensorCloudModel;

var logger = require('./logger.js');

var utils = {
  getSensorIDs: function (data) {
    return _.uniq(_.pluck(data, "sensor_id"));
  }
};

module.exports = utils;

var ibm = new IBM(config.ibmBluemix);
var azure = new Azure(config.microsoftAzure);

// Steps
// 1. Set Interval to 60 seconds
// 2. Get the values for each sensor
// 4. Get the cloud providers for each sensor
// 5. Group data by cloudprovider_id
// 6. Write the data to the cloud

// This server retrieves data from the cloud on configurable interval
setInterval(function() {
  var data = DataModel.find({}, {'_id':0, '__v':0}, function (err, data) {
    if(err) {
      console.log(err);
    } else {
      if(data.length == 0) {
        console.log("No sensor data in database");
        return;
      }
      // console.log(data);
    }

    // Retrieve all relations between sensors and clouds
    SensorCloudModel
    .find({}, {'_id':0, '__v':0},
    function(err, results) {
      console.log("Sensor Cloud results");
      logger.info("Database returned " + results.length + " sensorReadings");

      // Group all the data by sensor_id
      var data_by_sensorid = _.groupBy(
        data,
        function(k) {
          return  k.sensor_id;
        });

        if(data_by_sensorid){
          console.log('Data by Sensor IDs: ');
          console.log(data_by_sensorid);
        } else return;

        // Group relations by sensor_id
        var ids_by_cloudprovider = _.groupBy(
          results,
          function(k) {
            return  k.cloudprovider_id;
          });

          console.log('Data Relations by Cloud Provider: ');
          console.log(ids_by_cloudprovider);

          _.forEach(
            ids_by_cloudprovider,
            function(data, cloudprovider_id) {
              if(!data) return;
              logger.info("Cloudprovider_id:", cloudprovider_id);
              logger.info(JSON.stringify(data, null, ' '));
              logger.info("----------------------------------------");

              _.forEach(data, function(sensor_cloud) {
                if(!sensor_cloud) return;
                if(!sensor_cloud.sensor_id) return;
                if(!data_by_sensorid[sensor_cloud.sensor_id]) return;
                if (cloudprovider_id == 1) {
                  logger.info("Writing to Microsoft Azure");
                  console.log(data_by_sensorid[sensor_cloud.sensor_id]);
                  azure.write(data_by_sensorid[sensor_cloud.sensor_id]);
                } else if (cloudprovider_id == 2) {
                  ibm.write(data_by_sensorid[sensor_cloud.sensor_id]);
                  logger.info("Writing to IBM BlueMix");
                  console.log(data_by_sensorid[sensor_cloud.sensor_id]);
                }
              });
            });
            logger.info("Deleting all sensor readings from the Database");

            DataModel.remove({}, function(err) {
              if (err) {
                logger.error(err);
              } else {
                logger.info('Removed all Data from MongoDB');
              }
            });
          });
        });
      }, config.interval);
