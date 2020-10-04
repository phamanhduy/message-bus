/* eslint-disable max-classes-per-file */
const _ = require('lodash');

const Config = {};
class Queue {
  constructor(builder) {
    this.name = builder.name;
    this.owner = builder.owner;
    this.connector = builder.connector;
    Config.sns = this.connector.getSNS();
    Config.sqs = this.connector.getSQS();
  }

  async create() {
    const params = {
      QueueName: Utils.getEventHandlerQueueName(this.owner, this.name),
      Attributes: {
        MessageRetentionPeriod: '1209600', // 14 days
        VisibilityTimeout: `${5 * 60}`, // 5 minutes
      },
    };
    const _self = this;
    return new Promise((resolve, reject) => {
      Config.sqs.createQueue(params, (err, data) => {
        if (err) {
          reject(err);
          return;
        }
        _self.url = data.QueueUrl;
        Utils.addPermissions(data.QueueUrl).then((addPermError) => {
          if (addPermError) {
            return reject(addPermError);
          }
          return resolve(data);
        }).catch(reject);
      });
    });
  }

  async handleMessage(config, callback) {
    const maxMessage = _.get(config, 'maxMessage', 1);
    const ccu = _.get(config, 'ccu', 1);
    const handleParams = {
      QueueUrl: this.url,
      AttributeNames: ['All'],
      MaxNumberOfMessages: maxMessage,
      MessageAttributeNames: [
        'All',
      ],
      VisibilityTimeout: '10',
      WaitTimeSeconds: 20,
    };
    for (let i = 0; i < ccu; i += 1) {
      this.poll(handleParams, callback, {
        index: i,
        config,
      });
    }
  }

  async removeMessage(message) {
    return new Promise((resolve, reject) => {
      Config.sqs.deleteMessage({
        QueueUrl: this.url,
        ReceiptHandle: message.ReceiptHandle,
      }, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async getArn() {
    const _self = this;
    if (!this.url) {
      return Promise.reject();
    }
    if (this.arn) {
      return Promise.resolve(this.arn);
    }
    return Utils.getArn(this.url).then((arn) => {
      _self.arn = arn;
      return arn;
    });
  }

  async subscribeToEvent(eventName, snsPrefix) {
    const prefix = snsPrefix || 'arn:aws:sns:ap-southeast-1:793754848903';
    const topicArn = `${prefix}:${eventName}`;
    const queueArn = await this.getArn();
    const params = {
      Protocol: 'sqs',
      TopicArn: topicArn,
      Endpoint: queueArn,
    };
    return new Promise((resolve, reject) => {
      Config.sns.subscribe(params, (err, data) => {
        if (err) {
          return reject(err);
        }
        return resolve(data);
      });
    });
  }


  async poll(handleParams, callback, context) {
    const _self = this;
    Config.sqs.receiveMessage(handleParams, async (err, response) => {
      if (err) {
        _self.poll(handleParams, callback, context);
        return;
      }
      try {
        await _self.processMessage(response, callback, context);
      } finally {
        _self.poll(handleParams, callback, context);
      }
    });
  }

  async processMessage(response, callback, context = {}) {
    if (_.isEmpty(_.get(response, 'Messages'))) {
      return [];
    }
    const _self = this;
    return Promise.all(_.map(response.Messages, async message => {
      const parsedMessage = JSON.parse(JSON.parse(message.Body).Message);
      if (maxRetry(message, context.config)) {
        if (_.isFunction(_.get(context, 'config.failedRetry'))) {
          context.config.failedRetry(parsedMessage);
          _self.removeMessage(message);
        }
        return;
      }
      await callback(parsedMessage);
      _self.removeMessage(message);
    }));
  }

  static get Builder() {
    class Builder {
      name(name) {
        this.name = name;
        return this;
      }

      owner(owner) {
        this.owner = owner;
        return this;
      }

      connector(connector) {
        this.connector = connector;
        return this;
      }

      build() {
        return new Queue(this);
      }
    }
    return Builder;
  }
}

function maxRetry(message, config) {
  return parseInt(_.get(message, 'Attributes.ApproximateReceiveCount', 0), 10)
  > parseInt(_.get(config, 'retryCount', 10), 10);
}


const Utils = {
  async getArn(url) {
    return new Promise((resolve, reject) => {
      if (!url) {
        reject();
      }
      const params = {
        QueueUrl: url,
        AttributeNames: ['QueueArn'],
      };
      Config.sqs.getQueueAttributes(params, (err, data) => {
        if (err) {
          return reject(err);
        }
        return resolve(data.Attributes.QueueArn);
      });
    });
  },

  async addPermissions(url) {
    const queueArn = await Utils.getArn(url);
    const policy = {
      Id: `SQS${queueArn}`,
      Statement: {
        Sid: '1',
        Effect: 'Allow',
        Principal: '*',
        Action: 'sqs:*',
        Resource: queueArn,
      },
    };
    return new Promise(resolve => {
      Config.sqs.setQueueAttributes({
        QueueUrl: url,
        Attributes: { Policy: JSON.stringify(policy) },
      }, (err, data) => {
        resolve(err, data);
      });
    });
  },

  getEventHandlerQueueName(consumerName, eventName) {
    return `messagebus_${consumerName}___${eventName}`;
  },
};


module.exports = Queue;
