const AWSConnector = require('./connector/aws');
const { Queue } = require('../base');

class AntBusConsumer {
  constructor(awsConfig) {
    this.connector = new AWSConnector(awsConfig);
    this.sns = this.connector.getSNS();
    this.sqs = this.connector.getSQS();
  }

  register(consumerName) {
    const _self = this;
    return new Promise((resolve, reject) => {
      _self._validateConsumerName(consumerName).then(() => {
        _self.consumerName = consumerName;
        resolve();
      }).catch(reject);
    });
  }

  async on(eventName, config, callback) {
    const queue = new Queue.Builder()
      .name(eventName)
      .owner(this.consumerName)
      .connector(this.connector)
      .build();
    queue.create().then(() => {
      queue.subscribeToEvent(eventName, config.snsAddress);
    }).then(() => {
      queue.handleMessage(config, callback);
    });
  }

  async _validateConsumerName(consumerName) {
    const _self = this;
    return new Promise((resolve) => {
      _self.sqs.listQueues({
        QueueNamePrefix: _getEventHandlerQueuePrefix(consumerName),
      }, (err) => {
        if (err) {
          return resolve(false);
        }
        return resolve(true);
      });
    });
  }
}

function _getEventHandlerQueuePrefix(consumerName) {
  return `messagebus_${consumerName}___`;
}

module.exports = AntBusConsumer;
