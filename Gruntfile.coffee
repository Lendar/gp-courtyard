request = require 'request'
fs = require 'fs'
dirty = require 'dirty'
geocache = dirty 'geocache.db'
geocache.on 'load', -> geocache.loaded = 1

queryAddress = (address, callback) ->
  cached_body = geocache.get address
  if cached_body
    return callback null, cached_body
  url = "https://maps.googleapis.com/maps/api/geocode/json"
  qs = {address, sensor: 'false'}
  setTimeout ->
    request {url, qs, json: true}, (err, res, body) ->
      geocache.set address, body
      callback err, body
  , 2000

processRecord = (record, done) ->
  address_hint = 'Москва '
  address = record.address
  console.log '🔎 ', address
  queryAddress address_hint + address, (err, body) ->
    if err
      done err
    else
      # console.log body?.results?[0]?.geometry
      # location = body?.results?[0]?.geometry?.viewport?.northeast
      location = body?.results?[0]?.geometry?.location
      if location?.lat? and location?.lng?
        # record.lat = location.lat
        # record.lng = location.lng
        done null, {record, lat: location.lat, lng: location.lng}
      else
        done null, null

processRecords = (records, done, results=[]) ->
  if records.length
    record = records.pop()
    processRecord record, (err, result) ->
      if err
        done err
      else
        if result
          results.push result
        processRecords records, done, results
  else
    done null, results

gridRect = (lng, lat, size) ->
  sizex = size
  sizey = size / 2
  xi = Math.floor(lng / sizex)
  yi = Math.floor(lat / sizey)
  x1 = xi * sizex
  x2 = x1 + sizex
  y1 = yi * sizey
  y2 = y1 + sizey
  [[x1, y1], [x1, y2], [x2, y2], [x2, y1], [x1, y1]]


columns =
  'Адрес вашего двора': 'address'
  'Оцените по шкале от 1 до 10 качество озеленения у вас во дворе': 'zelen'
numeric_columns = ['zelen']

module.exports = (grunt) ->

  grunt.initConfig
    load_csv:
      poll:
        files: [
          src: 'poll1685.csv'
        ]
    save_geojson:
      poll:
        files: [
          dest: 'poll1685.geojson'
        ]

  grunt.registerMultiTask 'load_csv', 'Load poll CSV', ->
    done = @async()
    csv = require 'csv'
    filename = @files[0].src[0]
    grunt.log.writeln 'Load CSV', filename
    csv()
    .from.path(filename, columns: true)
    .transform (row) ->
      new_row = {}
      for key, value of row
        to = columns[key]
        if to
          value = parseInt value if to is 'zelen'
          new_row[to] = value
      return new_row
    .to.array (records) ->
      grunt.log.writeln 'Read', records.length, 'records'
      grunt.config.set 'records', records
      done()

  grunt.registerTask 'geocode', 'Try Logging', ->
    done = @async()
    setTimeout ->
      unless geocache.loaded
        grunt.fail.fatal 'Geocache is not ready'
      grunt.config.requires 'records'
      records = grunt.config.get 'records'
      grunt.log.writeln 'Geocode', records?.length, 'records'
      processRecords records, (err, points) ->
        if err
          grunt.fail.fatal 'No records'
        else
          grunt.config.set 'points', points
        done err
    , 1000

  grunt.registerMultiTask 'save_geojson', 'Save points to .geojson', ->
    done = @async()
    grunt.config.requires 'points'
    points = grunt.config.get 'points'
    dest = @files[0].dest
    unless dest?
      grunt.fail.fatal 'No dest'
    grunt.log.writeln 'Save', points?.length, 'points to', dest

    geo =
      type: 'FeatureCollection'
      features: ({
        type: 'Feature'
        geometry: {
          type: 'Point'
          coordinates: [lng, lat]
        }
        properties: record
      } for {lng, lat, record} in points).concat({
        type: 'Feature'
        geometry: {
          type: 'Polygon'
          coordinates: [gridRect(lng, lat, 0.006)]
        }
        properties: record
      } for {lng, lat, record} in points)

    fs.writeFile dest, JSON.stringify(geo), done

  grunt.registerTask 'default', [
    'load_csv'
    'geocode'
    'save_geojson'
  ]
