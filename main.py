#written by William Wiskes 3/30/2022, published under MIT
import arcgis
from arcgis.geometry import Geometry
from arcgis import GIS
import geopandas as gpd
import json
from pyproj import Proj, transform

def guzzlers(request):
    #define your connection to AGOL
    gis = GIS("https://utahdnr.maps.arcgis.com/", "REMOVED", "REMOVED")
    #load in your point layer to enrich
    point = gis.content.get('f81d5fe4c0dc4068a93d595d0e64e43e') 
    #declare your point sub layer - will be [0] unless the service has multiple layers
    point_lyr = point.layers[0]
    #query your layer, to not overload cloud functions I limit all queries to 50 records, if there are more than 50 records to update those will be enriched 
    #in subsiquent cron runs of this script. If there are no records to update, this script ends saving resources
    fset_point = point_lyr.query(where='County is null', result_record_count=50, return_all_records=False) 
    if not fset_point:
        fset_point = point_lyr.query(where='Guzzler_ID is null', result_record_count=50, return_all_records=False)
    if not fset_point:
        fset_point = point_lyr.query(where='Land_ownership is null', result_record_count=50, return_all_records=False)
    if fset_point:
        # get layers to enrich off of
        poly = gis.content.get('def1c0a6b6214e9f8858dfce69b6d038') #this is prominent features
        poly_lyr = poly.layers[0]
        fset_poly = poly_lyr.query(out_sr={'wkid': 3857})
        county = gis.content.get('0646d9190db941b682fd57fb7072f55b') #counties
        county_lyr = county.layers[0]
        fset_county = county_lyr.query()
        region = gis.content.get('70b2a33851eb4b58a7174c7464e3226a') #regions
        region_lyr = region.layers[0]
        fset_region = region_lyr.query()
        prop = gis.content.get('7a33f9b893624867b57371bd4c95d41f') #property ownership
        prop_lyr = prop.layers[0]
        fset_prop = prop_lyr.query()
        blm = gis.content.get('e023b089b7584c07844a15e43e782f8e') #blm ownership
        blm_lyr = blm.layers[0]
        fset_blm = blm_lyr.query()
        #start add "feature names"
        #convert the features to geojson 
        gjson_string = fset_poly.to_geojson
        gjson_dict = json.loads(gjson_string)
        poly_gdf = gpd.GeoDataFrame.from_features(gjson_dict['features'])
        gjson_string = fset_point.to_geojson
        gjson_dict = json.loads(gjson_string)
        point_gdf = gpd.GeoDataFrame.from_features(gjson_dict['features'])
        #here we are using the geopandas package to perform a spatial join. 
        result = gpd.tools.sjoin(point_gdf, poly_gdf, how="left")
        #we then concatinate the object ID with the region code to make a unique ID for each feature.
        result['Guzzler_ID'] = result['Code'].astype(str) + result['OBJECTID_left'].astype(str)
        result['point'] = result['geometry'].astype(str)
        #clean up the columns
        results = result[['geometry', 'OBJECTID_left', 'Land_Ownership', 'DWR_Region', 'BLM_Field_Office', 'County', 'UTM_N', 'UTM_E', 'Guzzler_ID', 'point', 'Name']]
        clean = results.rename(columns ={'Name':'feature', 'OBJECTID_left':'OBJECTID'}) 
        clean['feature'] = clean['feature'].astype(str)
        #start add "counties"
        #convert the features to geojson 
        gjson_string = fset_county.to_geojson
        gjson_dict = json.loads(gjson_string)
        county_gdf = gpd.GeoDataFrame.from_features(gjson_dict['features'])
        #spatial join
        result2 = gpd.tools.sjoin(clean, county_gdf, how="left")
        #clean columns
        results2 = result2[['geometry', 'OBJECTID_left', 'Land_Ownership', 'DWR_Region', 'BLM_Field_Office', 'UTM_N', 'UTM_E', 'Guzzler_ID', 'point', 'NAME']]
        clean2 = results2.rename(columns ={'OBJECTID_left':'OBJECTID', 'NAME':'County'})
        clean2['County'] = clean2['County'].astype(str)
        #start add "region"
        gjson_string = fset_region.to_geojson
        gjson_dict = json.loads(gjson_string)
        region_gdf = gpd.GeoDataFrame.from_features(gjson_dict['features'])
        #spatial join
        result3 = gpd.tools.sjoin(clean2, region_gdf, how="left")
        #clean columns
        results3 = result3[['geometry', 'OBJECTID', 'Land_Ownership', 'BLM_Field_Office', 'UTM_N', 'UTM_E', 'Guzzler_ID', 'point', 'County', 'DWR_REGION']]
        clean3 = results3.rename(columns ={'DWR_REGION':'DWR_Region'})
        clean3['DWR_Region'] = clean3['DWR_Region'].astype(str)
        #start add "property ownership"
        gjson_string = fset_prop.to_geojson
        gjson_dict = json.loads(gjson_string)
        prop_gdf = gpd.GeoDataFrame.from_features(gjson_dict['features'])
        #spatial join
        result4 = gpd.tools.sjoin(clean3, prop_gdf, how="left")
        #clean columns
        results4 = result4[['geometry', 'OBJECTID_left', 'DWR_Region', 'BLM_Field_Office', 'UTM_N', 'UTM_E', 'Guzzler_ID', 'point', 'County', 'AGENCY']]
        clean4 = results4.rename(columns ={'OBJECTID_left':'OBJECTID', 'AGENCY':'Land_Ownership'})
        clean4['Land_Ownership'] = clean4['Land_Ownership'].astype(str)
        #start add "blm ownership"
        gjson_string = fset_blm.to_geojson
        gjson_dict = json.loads(gjson_string)
        blm_gdf = gpd.GeoDataFrame.from_features(gjson_dict['features'])
        #spatial join
        result5 = gpd.tools.sjoin(clean4, blm_gdf, how="left")
        #clean columns
        results5 = result5[['geometry', 'OBJECTID_left', 'DWR_Region', 'UTM_N', 'UTM_E', 'Guzzler_ID', 'point', 'County', 'Land_Ownership', 'ADMU_NAME']]
        clean5 = results5.rename(columns ={'OBJECTID_left':'OBJECTID', 'ADMU_NAME':'BLM_Field_Office'})
        clean5['BLM_Field_Office'] = clean5['BLM_Field_Office'].astype(str)
        cleanjs = json.loads(clean5.to_json())
        parsed = cleanjs['features']
        #we need our point data stored in multiple projections
        #here we are using the pyproj library to make those reprojections
        inProj = Proj(init='epsg:102100') #ESRI web
        outProj = Proj(init='epsg:26912') #UTM zone 12
        outProj2 = Proj(init='epsg:4326') #wgs84
        #declare a number to count up from and the end number (total)
        i = 0
        total = len(fset_point.features)
        #loop through each feature sending edits to AGOL
        #the first loop is to loop through the features from AGOL, updating them,
        #the second (nested) loop is to loop through the enriched features 
        while i < total:
            edit_feature = fset_point.features[i]
            p = 0
            ptotal = len(parsed)
            while p < ptotal:
                #if the enriched features objectID matches that of the AGOL feature, then update it
                if parsed[p]['properties']['OBJECTID'] == edit_feature.attributes['OBJECTID']:
                    guz_id = parsed[p]['properties']['Guzzler_ID']
                    edit_feature.attributes['Guzzler_ID'] = guz_id #Guzzler ID
                    county = parsed[p]['properties']['County']
                    edit_feature.attributes['County'] = county #county
                    region = parsed[p]['properties']['DWR_Region']
                    edit_feature.attributes['DWR_Region'] = region #region
                    ownership = parsed[p]['properties']['Land_Ownership']
                    edit_feature.attributes['Land_Ownership'] = ownership #land ownership
                    blm = parsed[p]['properties']['BLM_Field_Office']
                    edit_feature.attributes['BLM_Field_Office'] = blm # BLM
                    utmx = parsed[p]['properties']['point'][7:].split(" ", 1)[0] 
                    utmy = parsed[p]['properties']['point'][7:].split(" ", 1)[1].replace(")", "") 
                    utm = transform(inProj,outProj,utmx,utmy)
                    edit_feature.attributes['UTM_E'] = utm[0] #UTM projected point X
                    edit_feature.attributes['UTM_N'] = utm[1] #UTM projected point Y
                    gps = transform(inProj,outProj2,utmx,utmy)
                    edit_feature.attributes['GPS'] = str(gps[1]) + ',' + str(gps[0]) #concatinated WGS84 
                    point_lyr.edit_features(updates=[edit_feature]) #update the feature
                p = p + 1
            i = i + 1
        return "complete"
    else:
        return "no data"