/* eslint-disable class-methods-use-this */
const AWS = require('aws-sdk');

class AWSConnector {
  constructor(awsConfig) {
    AWS.config.update(awsConfig);
  }

  getSNS() {
    return new AWS.SNS();
  }

  getSQS() {
    return new AWS.SQS();
  }
}
module.exports = AWSConnector;
