const express = require('express');
const app = express();
const {verifyToken} = require('../middlewares/authotization');

const ReporteMasterBdCtrl=require('../controllers/ReporteMasterBdCtrl');

const CrearReporteMasterBd='/api/rep_crear_master_bd_execute';
const GetMasterBd='/api/get_master_bd';

app.post(`${CrearReporteMasterBd}`,[],ReporteMasterBdCtrl.CrearReporteMasterBd);
app.get(`${GetMasterBd}`,[],ReporteMasterBdCtrl.GetMasterBd);
/************************************************************/
module.exports=app;