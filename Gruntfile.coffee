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
  address = record[5]
  console.log '🔎 ', address
  queryAddress address_hint + address, (err, body) ->
    if err
      done err
    else
      # console.log body?.results?[0]?.geometry
      # location = body?.results?[0]?.geometry?.viewport?.northeast
      location = body?.results?[0]?.geometry?.location
      if location?.lat? and location?.lng?
        done null, location
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

module.exports = (grunt) ->

  grunt.initConfig
    load_csv:
      poll:
        files: [
          src: 'poll357.csv'
        ]
    save_geojson:
      poll:
        files: [
          dest: 'poll357.geojson'
        ]

  grunt.registerMultiTask 'load_csv', 'Load poll CSV', ->
    done = @async()
    csv = require 'csv'
    filename = @files[0].src[0]
    grunt.log.writeln 'Load CSV', filename
    csv()
    .from.path(filename, columns: false)
    .to.array (records) ->
      grunt.log.writeln 'Read', records.length, 'records'
      grunt.config.set 'records', records
      done()

  grunt.registerTask 'geocode', 'Try Logging', ->
    done = @async()
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
        properties:
          prop0: '1'
      } for {lng, lat} in points)

    fs.writeFile dest, JSON.stringify(geo), done

  grunt.registerTask 'default', [
    'load_csv'
    'geocode'
    'save_geojson'
  ]
