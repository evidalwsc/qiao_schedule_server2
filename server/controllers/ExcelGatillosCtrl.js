const client = require('../config/db.client');
const jwt=require('jsonwebtoken');
var path = require('path');
const axios = require('axios');
const fs = require("fs");
const PDFDocument = require('pdfkit');
const cryptoJs = require('crypto-js');
const funcionesCompartidasCtrl = require('./funcionesCompartidasCtrl.js');

exports.DownloadReporteGatilo = async (req,res) =>{ try {

    const Read_fs = require("fs");
    const Read_directory = "./public/files/exceldespachos/";
    const Read_path = require("path");

    res.download('./public/files/exceldespachos/reporte_gatillos.xls');

} catch (error) {
    console.log("ERROR "+error);
    res.status(400).send({
        message: "ERROR AL CARGAR EL ARCHIVO",
        success:false,
    }); res.end(); res.connection.destroy();
}};

exports.DownloadReporteGatilo2= async (req,res) =>{ try {

    res.status(200).send({
        message: "CORRECTO",
        success:true,
    }); res.end(); res.connection.destroy();

} catch (error) {
    console.log("ERROR "+error);
    res.status(400).send({
        message: "ERROR AL CARGAR EL ARCHIVO",
        success:false,
    }); res.end(); res.connection.destroy();
}};