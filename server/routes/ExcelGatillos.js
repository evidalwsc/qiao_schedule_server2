const express = require('express');
const app = express();
const {verifyToken} = require('../middlewares/authotization');

const ExcelGatillosCtrl=require('../controllers/ExcelGatillosCtrl');
const DownloadReporteGatilo='/api/get_reporte_gatillos';

app.get(`${DownloadReporteGatilo}`,[],ExcelGatillosCtrl.DownloadReporteGatilo);
/************************************************************/
module.exports=app;
