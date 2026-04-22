class ViewerError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ViewerError';
  }
}

module.exports = {
  ViewerError,
};
