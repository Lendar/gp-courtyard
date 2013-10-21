request = require 'request'
{exec} = require 'child_process'
fs = require 'fs'
path = require 'path'
gju = require 'geojson-utils'
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
      precision = body?.results?[0]?.geometry?.location_type
      if location?.lat? and location?.lng?
        # record.lat = location.lat
        # record.lng = location.lng
        record.precision = precision
        done null, {record, lat: location.lat, lng: location.lng, has_address: !!address}
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
  'Оцените по шкале от 1 до 10 загруженность вашего двора запаркованными автомобилями?': 'car'
  'Оцените по шкале от 1 до 10 качество озеленения у вас во дворе': 'zelen'
  'Оцените по шкале от 1 до 10 приспособленность вашего двора для маломобильных групп населения - пенсионеров, инвалидов, родителей с детскими колясками, велосипедистов?': 'access'
  'По шкале от 1 до 10, принимая во внимание все характеристики двора, оцените ваш двор': 'total'
numeric_columns = ['zelen', 'car', 'access', 'total']

module.exports = (grunt) ->
  _ = grunt.util._

  grunt.initConfig
    load_csv:
      poll:
        files: [
          src: 'poll1794.csv'
        ]
    save_geojson:
      poll:
        files: [
          dest: 'gen/poll1794.geojson'
        ]
    query_arcgis:
      districts:
        dest: 'gen/districts.geojson.part'
        urls: [
          'http://api.atlas.mos.ru/arcgis/rest/services/Basemaps/egipdata/MapServer/223/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&maxAllowableOffset=305&geometry=%7B%22xmin%22%3A3913575.848189838%2C%22ymin%22%3A7511529.185500466%2C%22xmax%22%3A4070118.882117804%2C%22ymax%22%3A7668072.219428432%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&callback=dojo.io.script.jsonp_dojoIoScript_Granici_rayonov_EDS2234_305_7481131405579__1_153._jsonpCallback'
          'http://api.atlas.mos.ru/arcgis/rest/services/Basemaps/egipdata/MapServer/223/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&maxAllowableOffset=305&geometry=%7B%22xmin%22%3A4070118.882117804%2C%22ymin%22%3A7511529.185500466%2C%22xmax%22%3A4226661.91604577%2C%22ymax%22%3A7668072.219428432%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&callback=dojo.io.script.jsonp_dojoIoScript_Granici_rayonov_EDS2234_305_7481131405579__1_154._jsonpCallback'
          'http://api.atlas.mos.ru/arcgis/rest/services/Basemaps/egipdata/MapServer/223/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&maxAllowableOffset=305&geometry=%7B%22xmin%22%3A4226661.91604577%2C%22ymin%22%3A7511529.185500466%2C%22xmax%22%3A4383204.949973736%2C%22ymax%22%3A7668072.219428432%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&callback=dojo.io.script.jsonp_dojoIoScript_Granici_rayonov_EDS2234_305_7481131405579__1_155._jsonpCallback'
          'http://api.atlas.mos.ru/arcgis/rest/services/Basemaps/egipdata/MapServer/223/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&maxAllowableOffset=305&geometry=%7B%22xmin%22%3A3913575.848189838%2C%22ymin%22%3A7354986.1515725%2C%22xmax%22%3A4070118.882117804%2C%22ymax%22%3A7511529.185500466%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&callback=dojo.io.script.jsonp_dojoIoScript_Granici_rayonov_EDS2234_305_7481131405579_0_153._jsonpCallback'
          'http://api.atlas.mos.ru/arcgis/rest/services/Basemaps/egipdata/MapServer/223/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&maxAllowableOffset=305&geometry=%7B%22xmin%22%3A4070118.882117804%2C%22ymin%22%3A7354986.1515725%2C%22xmax%22%3A4226661.91604577%2C%22ymax%22%3A7511529.185500466%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&callback=dojo.io.script.jsonp_dojoIoScript_Granici_rayonov_EDS2234_305_7481131405579_0_154._jsonpCallback'
          'http://api.atlas.mos.ru/arcgis/rest/services/Basemaps/egipdata/MapServer/223/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&maxAllowableOffset=305&geometry=%7B%22xmin%22%3A4226661.91604577%2C%22ymin%22%3A7354986.1515725%2C%22xmax%22%3A4383204.949973736%2C%22ymax%22%3A7511529.185500466%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&callback=dojo.io.script.jsonp_dojoIoScript_Granici_rayonov_EDS2234_305_7481131405579_0_155._jsonpCallback'
        ]
    merge_geojson:
      districts:
        files: [{
          src: 'gen/districts.geojson.part*'
          dest: 'gen/districts.geojson'
          pretty: true
        }]
    add_column:
      districts:
        files: [{
          points: 'gen/poll1794.geojson'
          polygons: 'gen/districts.geojson'
          dest: 'gen/poll1794-districts.geojson'
          property: 'district'
        }]


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
        to = columns[key.trim()]
        if to
          value = parseInt value if to in numeric_columns
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
    @files.forEach (file) ->
      geo =
        type: 'FeatureCollection'
        features: ({
          type: 'Feature'
          geometry: {
            type: 'Point'
            coordinates: [lng, lat]
          }
          properties: record
        } for {lng, lat, record, has_address} in points when has_address and record.precision isnt 'APPROXIMATE')

      if file.rect
        geo.features = geo.features.concat({
          type: 'Feature'
          geometry: {
            type: 'Polygon'
            coordinates: [gridRect(lng, lat, 0.006)]
          }
          properties: record
        } for {lng, lat, record} in points)

      grunt.log.writeln 'Save', geo.features.length, 'points to', file.dest
      grunt.file.mkdir path.dirname file.dest
      grunt.file.write file.dest, JSON.stringify(geo), done

  grunt.registerMultiTask 'query_arcgis', 'Query Arcgis servers', ->
    done = @async()
    urls = @data.urls
    dest = @data.dest
    left = urls.length
    grunt.log.writeln 'Run ogr2ogr for', left, 'zones'
    grunt.file.mkdir path.dirname dest
    for url, index in urls
      url = url.replace(/&callback=.*$/, '')
      filepath = "#{dest}#{index}"
      grunt.log.writeln 'Download to', filepath
      grunt.file.delete "#{dest}#{index}"
      cmd = "ogr2ogr -f GeoJSON #{dest}#{index} \"#{url}\""
      grunt.verbose.writeln 'Run', cmd
      exec cmd, (err, res) ->
        if err
          grunt.fail.warn err
        if res
          grunt.log.writeln res
        left--
        done() unless left
        grunt.verbose.writeln 'Left', left

  grunt.registerMultiTask 'merge_geojson', 'Merge geojson features', ->
    grunt.verbose.writeln 'merge', @files.length
    @files.forEach (file) ->
      merged_json = file.src.map (filepath) ->
        grunt.verbose.writeln 'read', filepath
        grunt.file.readJSON filepath
      .reduce (a, b) ->
        c = _.clone a
        c.features = [].concat a.features, b.features
        return c
      grunt.log.writeln 'Save', file.dest
      raw = if file.pretty
        JSON.stringify(merged_json, null, 2)
      else
        JSON.stringify(merged_json)
      grunt.file.write file.dest, raw

  grunt.registerMultiTask 'add_column', 'Add district property to points', ->
    @files.forEach (file) =>
      points = grunt.file.readJSON file.points
      polygons = grunt.file.readJSON file.polygons
      grunt.log.writeln 'Read', points.features.length, 'points'
      grunt.log.writeln 'Read', polygons.features.length, 'polygons'
      stat = {matched: 0, total: 0}
      for point in points.features
        grunt.verbose.debug 'poly', polygons.features[0].geometry.coordinates
        grunt.verbose.debug 'point', point.geometry
        matches =
          for poly in polygons.features when gju.pointInPolygon point.geometry, poly.geometry
            poly
        grunt.verbose.debug 'match', matches.length, '/', polygons.features.length, point.properties.address
        stat.total++
        if matches.length
          grunt.log.debug 'Match', matches[0]
          point.properties[file.property] = matches[0]
          stat.matched++
      grunt.log.writeln "Processed: #{stat.total}. Found: #{stat.matched}."
      grunt.file.write file.dest, JSON.stringify(points)

  grunt.registerTask 'default', [
    'load_csv'
    'geocode'
    'save_geojson'
    'query_arcgis:districts'
    'merge_geojson:districts'
    'add_column:districts'
  ]
