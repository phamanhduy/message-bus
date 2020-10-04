class MiddleWareManager {
  constructor() {
    this.middlewares = [];
  }

  add(middleware) {
    this.middlewares = this.middlewares.concat(middleware);
  }

  execute(event, ...params) {
    this.middlewares.forEach((mw) => {
      if (mw[event] && typeof (mw[event]) === 'function') {
        mw[event].call(mw, params);
      }
    });
  }
}
module.exports = MiddleWareManager;
