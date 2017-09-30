
// SETUP
//
var CRS="EPSG:4326"
var SCALE=27.829872698318393
var COMPOSITE_IMG_NAME='HansenComposite_16'

var annual = ee.Image('UMD/hansen/global_forest_change_2016_v1_4')
//

// geoms
var geoms=ee.FeatureCollection('ft:13BvM9v1Rzr90Ykf1bzPgbYvbb8kGSvwyqyDwO8NI')

var get_flat_geom=function(name){
  var coords=ee.Feature(geoms.filter(ee.Filter.eq('name',name)).first()).geometry().coordinates()
  return ee.Geometry.Polygon(coords,null,false)
}
var world_geom=get_flat_geom('hansen_world')


// Grab loss and tc2000 from current asset
var tc = annual.select(['treecover2000'])
var ly = annual.select(['lossyear'])

// GAIN (from old asset)
// not sure why this is required, but values are different
// this asset has gain values of 1/255, annual update has 1/0
var hgain=ee.Image('HANSEN/gfw2015_loss_tree_gain_threshold').select(['gain'])

var tree=function(thresh){
  return tc.updateMask(tc.gte(thresh)).rename(['tree_'+thresh])
}
var loss=function(thresh){
  return ly.updateMask(tc.gte(thresh)).rename(['loss_'+thresh])
}
var thresholds=[10,15,20,25,30,50,75]
var threshold_images=[]
for (var i=0; i<thresholds.length; i++) {
  threshold_images.push(tree(thresholds[i]))
  threshold_images.push(loss(thresholds[i]))
}
var threshold_image=ee.Image(threshold_images)

//
// FINAL IMAGE
//
var hansen_composite=threshold_image.addBands([hgain])
Map.addLayer(hansen_composite,null,'HansenComposite')
print(hansen_composite)


//
// Exporter
//
var ppolicy={
  'tree_10':'mean',
  'loss_10':'mode',
  'tree_15':'mean',
  'loss_15':'mode',
  'tree_20':'mean',
  'loss_20':'mode',
  'tree_25':'mean',
  'loss_25':'mode',
  'tree_30':'mean',
  'loss_30':'mode',
  'tree_50':'mean',
  'loss_50':'mode',
  'tree_75':'mean',
  'loss_75':'mode',
  'gain':'mean'
}

var export_asset=function(img,name){
  Export.image.toAsset({
    'image':img,
    'description':name,
    'assetId':'projects/wri-datalab/'+name,
    'scale':SCALE,
    'crs':CRS,
    'region':world_geom.coordinates(),
    'pyramidingPolicy':ppolicy,
    'maxPixels':800000000000
  })
}

export_asset(hansen_composite,COMPOSITE_IMG_NAME)
