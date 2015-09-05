var data = {
  labels: ['Mapreduce', 'HBase', 'Hive', 'Spark', 'BigSQL', 'R'],
  series: [45, 29, 21,3, 2, 1]
};

var options = {
  labelInterpolationFnc: function(value) {
    return value[0]
  }
};

var responsiveOptions = [
  ['screen and (min-width: 200px)', {
    chartPadding: 20,
    labelOffset: 20,
    labelDirection: 'explode',
    labelInterpolationFnc: function(value) {
      return value;
    }
  }],
  ['screen and (min-width: 400px)', {
    labelOffset: 80,
    chartPadding: 20
  }]
];

new Chartist.Pie('.ct-chart', data, options, responsiveOptions);
