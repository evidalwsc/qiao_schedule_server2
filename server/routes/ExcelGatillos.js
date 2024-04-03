const express = require('express');
const app = express();
const {verifyToken} = require('../middlewares/authotization');

const ExcelGatillosCtrl=require('../controllers/ExcelGatillosCtrl');
const TareasCtrl=require('../controllers/tareas_programadas_ctrl');
const DownloadReporteGatilo='/api/get_reporte_gatillos';
const ReporteGatiloByApi='/api/post_api_reporte_gatillos';

app.get(`${DownloadReporteGatilo}`,[],ExcelGatillosCtrl.DownloadReporteGatilo);
app.post(`${ReporteGatiloByApi}`,[],TareasCtrl.GenerateReporteGatiloByAPI);
/************************************************************/
module.exports=app;
