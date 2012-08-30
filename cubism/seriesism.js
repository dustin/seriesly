// Seriesly plugin for cubism.
//
// Load this file and use seriesly.context()
// in place of your normal cubism.context()

 var seriesly = {
    context: function() {
        var context = cubism.context();

        function seriesly(baseUrl) {
                var source = {};

            source.metric = function(dbname, ptr, reducer, lbl) {
                return context.metric(function(start, stop, step, callback) {
                    d3.json(baseUrl + dbname +
                            "/_query?ptr=" + ptr +
                            "&reducer=" + reducer +
                            "&from=" + (+start) +
                            "&to=" + (+stop) +
                            "&group=" + step,
                            function(data) {
                                if (data) {
                                    var rv = [];
                                    for (var i = (+start); i < (+stop); i+=step) {
                                        rv.push(data[i] ? data[i][0] : NaN);
                                    }
                                    callback(null, rv);
                                }
                            });
                }, lbl || (dbname + " - " + reducer + "@" + ptr));
            };

            source.toString = function() {
                return baseUrl;
            };

            return source;
        }

        context.seriesly = seriesly;

        return context;
    }
};
