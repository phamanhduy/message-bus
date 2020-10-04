const _ = require('lodash');
const AWSConnector = require('./connector/aws');
const BaseHandler = require('./base.handler');
const { AntTopic } = require('../base');

class AntBusPublisher extends BaseHandler {
  constructor(awsConfig) {
    super();
    this.connector = new AWSConnector(awsConfig);
    this.routingData = {};
    this.sns = this.connector.getSNS();
  }

  async register(context) {
    const topic = new AntTopic.Builder().name(context.name).connector(this.connector).build();
    this.routingData[context.name] = topic;
    return topic.create();
  }

  async registerAll(contexts = []) {
    const _self = this;
    return Promise.all(_.map(contexts, context => _self.register(context)));
  }

  async publish(eventName, context) {
    const topic = _.get(this.routingData, eventName);
    if (!topic || !topic.isReady()) {
      throw new Error('NOT_READY');
    }
    return topic.publish(eventName, context);
  }
}
module.exports = AntBusPublisher;
