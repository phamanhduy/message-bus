/* eslint-disable max-classes-per-file */
const routingData = {};

const Config = {};
class AntTopic {
  constructor(builder) {
    this.name = builder.name;
    this.connector = builder.connector;
    this.config = Config;
    Config.sns = this.connector.getSNS();
    Config.sqs = this.connector.getSQS();
  }

  async create() {
    const _self = this;
    return new Promise((resolve, reject) => {
      Config.sns.createTopic({
        Name: this.name,
      }, (err, data) => {
        if (err) {
          reject(err);
        } else {
          routingData[_self.name] = data.TopicArn;
          resolve(data);
        }
      });
    });
  }

  isReady() {
    return routingData[this.name] !== undefined;
  }

  async publish(eventName, context) {
    return new Promise((resolve, reject) => {
      if (context == null || context.message == null || !routingData[eventName]) {
        return reject(new Error('No message or routing data'));
      }
      const params = {
        Message: context.message,
        TopicArn: routingData[eventName],
      };
      return this.config.sns.publish(params, (err, data) => {
        if (err) {
          reject(err, err.stack);
        } else {
          resolve(data);
        }
      });
    });
  }

  getArn() {
    const _self = this;
    return new Promise((resolve, reject) => {
      if (!this.url) {
        reject();
        return;
      }
      if (this.arn) {
        resolve(this.arn);
        return;
      }
      Config.sqs.getQueueAttributes({
        QueueUrl: this.url,
        AttributeNames: ['QueueArn'],
      }, (err, data) => {
        if (err) {
          return reject(err);
        }
        _self.arn = data.Attributes.QueueArn;
        return resolve(_self.arn);
      });
    });
  }

  static get Builder() {
    class Builder {
      name(name) {
        this.name = name;
        return this;
      }

      connector(connector) {
        this.connector = connector;
        return this;
      }

      build() {
        return new AntTopic(this);
      }
    }
    return Builder;
  }
}

module.exports = AntTopic;
