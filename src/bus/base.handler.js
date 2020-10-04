const MiddlewareManager = require('../middleware/manager');
class BaseHandler {
  constructor() {
    this.middlewareManager = new MiddlewareManager();
  }

  use(middleware) {
    this.middlewareManager.add(middleware);
  }
}
module.exports = BaseHandler;
