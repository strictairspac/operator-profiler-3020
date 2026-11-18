import {datasets,plans,traces,indexes} from '../api/business.js';
export const store={datasets:[],plans:[],traces:[],indexes:[],user:null};
export async function refresh(){[store.datasets,store.plans,store.traces,store.indexes]=await Promise.all([datasets.list(),plans.list(),traces.list(),indexes.list()]);}
