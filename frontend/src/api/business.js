import {api,post} from './client.js';
export const datasets={list:()=>api('/datasets'),save:(d,id)=>post('/datasets'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const plans={list:()=>api('/plans'),save:(d,id)=>post('/plans'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const traces={list:()=>api('/traces'),run:d=>post('/traces',d),detail:id=>api('/traces/'+id)};
export const indexes={list:datasetId=>api('/indexes'+(datasetId?('?dataset_id='+datasetId):'')),create:d=>post('/indexes',d),retry:id=>post('/indexes/'+id+'/retry'),remove:id=>api('/indexes/'+id,{method:'DELETE'})};
