export default [
  {
    match: {},
    callback: {
      url: 'http://search/update',
      method: 'POST'
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  },
  {
    match: {
      // form of element is {subject,predicate,object}
      predicate: {
        type: "uri",
        value: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
      },
      object: { type: "uri", value: "http://vocab.deri.ie/cogs#Job" },
    },
    callback: {
      url: "http://vocab-fetch/delta",
      method: "POST",
    },
    options: {
      resourceFormat: "v0.0.1",
      gracePeriod: 1000,
      ignoreFromSelf: true,
    },
  },
];
