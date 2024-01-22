const client = require('../config/db.client');
const jwt=require('jsonwebtoken');
var path = require('path');
const axios = require('axios');
const fs = require("fs");
const PDFDocument = require('pdfkit');
const cryptoJs = require('crypto-js');
const funcionesCompartidasCtrl = require('./funcionesCompartidasCtrl.js');

exports.CrearReporteMasterBd = async (req,res) =>{
try {

    const moment = require('moment');

    console.log('REPORTE MASTER BD 1');
    var MesesCerrados = await client.query(`
    SELECT 
    ing_fk_responsable
    , ing_fecha
    , del_fk_responsable
    , del_fecha
    , id
    , anio
    , mes
	FROM public.master_bd_mes_cerrado
    where
    del_fk_responsable=0
    `);

    var CondicioDelete = '';
    var CondicionSelect = '';

    console.log('\n CANTIDAD DE MESES CERRADOS '+JSON.stringify(MesesCerrados));

    if(MesesCerrados.rows.length>0)
    {
        for(var i=0; i<MesesCerrados.rows.length; i++) 
        {
            if(i==0)
            {
                CondicioDelete = ` where substring(n_carpeta,2,4)!='`+MesesCerrados.rows[i].anio.substring(2,4)+``+MesesCerrados.rows[i].mes+`' `;
            }
            else
            {
                CondicioDelete += ` and substring(n_carpeta,2,4)!='`+MesesCerrados.rows[i].anio.substring(2,4)+``+MesesCerrados.rows[i].mes+`' `;
            }

            CondicionSelect += ` and substring(d.n_carpeta,2,4)!='`+MesesCerrados.rows[i].anio.substring(2,4)+``+MesesCerrados.rows[i].mes+`' `;
        }
    }

    console.log('\n CONDICION DELETE '+JSON.stringify(CondicioDelete));

    console.log('\n CCONDICION SELECT '+JSON.stringify(CondicionSelect));

    console.log('\n INICIO CALCULO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
    
    console.log(`\nQUERY DELETE\n delete from public.master_bd `+CondicioDelete+` `);
    await client.query(` delete from public.master_bd `+CondicioDelete+` `);

    await client.query(`
    insert into public.master_bd (fecha_creacion, nc_id, n_carpeta, rut, contenedor, id_nave, nombre_nave, eta, m3, monto_din, monto_din_ajuste, monto_carga_usd, monto_carga_clp, monto_carga_clp_ajuste, total_gastos, total_provision, total_a_pagar, monto_pagado, ano, mes, base, aforo, cda, isp, pallets, tvp, otro_transporte, otros, detalle_otro, monto_aju_din, detalle_aju_din, monto_aju_serv, detalle_aju_serv, tc_servicio, precio_base, precio_unitario_x_m3, ejecutivo, mes_eta, din, fk_cliente, din_ingresada_fecha, nombre_cliente, din_pagada_flag, din_pagada_fecha, fk_servicio) 
    SELECT
    DISTINCT
    nc."createdAt" as fecha_creacion
    , nc.id as nc_id
    , coalesce(d.n_carpeta, '') as N_CARPETA
    , coalesce(c.rut, '') as RUT
    , coalesce(d.contenedor, '') as CONTENEDOR
    , coalesce(d.fk_nave, 0) as ID_NAVE
    , coalesce(d.nave_nombre, '[No ingresado]') as NOMBRE_NAVE
    , to_char(d.eta, 'DD-MM-YYYY') as ETA
    , ROUND(nc.carga, 2) as M3
    , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)) as MONTO_DIN
    , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)) as MONTO_DIN_AJUSTE
    , ROUND(coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0), 2) as MONTO_CARGA_USD
    , ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)) as MONTO_CARGA_CLP
    , ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0)) as MONTO_CARGA_CLP_AJUSTE
    , ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0) + coalesce(nc.aforo, 0)+coalesce(nc.isp, 0)+coalesce(nc.cda, 0)+coalesce(nc.almacenaje, 0) + coalesce(nc.pallet*nc.pallet_valor_u, 0)+coalesce(nc.ttvp, 0)+coalesce(nc.twsc, 0)+coalesce(nc.otros, 0)) as TOTAL_GASTOS
    , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)+(coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0)) as TOTAL_PROVISION
    , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)+(coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0) + coalesce(nc.aforo, 0)+coalesce(nc.isp, 0)+coalesce(nc.cda, 0)+coalesce(nc.almacenaje, 0) + coalesce(nc.pallet*nc.pallet_valor_u, 0)+coalesce(nc.ttvp, 0)+coalesce(nc.twsc, 0)+coalesce(nc.otros, 0)) as TOTAL_A_PAGAR
    , ROUND(coalesce((SELECT SUM(dtl.debit_amt) FROM public.wsc_envio_asientos_detalles dtl INNER JOIN public.wsc_envio_asientos_cabeceras cbc ON dtl.fk_cabecera=cbc.id WHERE cbc.carpeta=d.n_carpeta and cbc.estado<>'false' and dtl.desc_text='BANCO SANTANDER'), 0)) as MONTO_PAGADO
    , SUBSTRING(d.n_carpeta, 2, 2) as ANO
    , SUBSTRING(d.n_carpeta, 4, 2) as MES
    , SUBSTRING(d.n_carpeta, 1, 7) as BASE
    , nc.aforo as AFORO
    , nc.cda as CDA
    , nc.isp as ISP
    , coalesce(nc.pallet, 0)*coalesce(nc.pallet_valor_u, 0) as PALLETS
    , coalesce(nc.ttvp, 0) as TVP
    , coalesce(nc.twsc, 0) as OTRO_TRANSPORTE
    , coalesce(nc.otros, 0) as otros
    , coalesce(nc.detalle_otro, '') as detalle_otro
    , coalesce(nc.ajuste_m_1, 0) as MONTO_AJU_DIN
    , coalesce(nc.ajuste_c_1, '') as DETALLE_AJU_DIN
    , coalesce(nc.ajuste_m_2, 0) as MONTO_AJU_SERV
    , coalesce(nc.ajuste_c_2, '') as DETALLE_AJU_SERV
    , ROUND(nc.tc2, 2) as TC_SERVICIO
    , nc.pb as PRECIO_BASE
    , nc.costo as PRECIO_UNITARIO_X_M3
    , coalesce(us.nombre, '') as EJECUTIVO
    , to_char(d.eta, 'MM') as MES_ETA
    , coalesce(nc.din, 0) as din
    , d.fk_cliente
    , coalesce(to_char(nc.din_ingresada_fecha, 'DD-MM-YYYY'), '') as din_ingresada_fecha
    , coalesce(c.codigo, '') as nombre_cliente
    ,
    CASE WHEN nc.din_pagada_flag=true
        THEN 'SI'
        ELSE 'NO'
    END as din_pagada_flag,
    CASE WHEN nc.din_pagada_flag=true
        THEN to_char(nc.din_pagada_fecha, 'DD-MM-YYYY')
        ELSE ''
    END as din_pagada_fecha
    ,
    coalesce((select ct.fk_consolidado
        from tracking t
        inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
        where fk_contenedor = (
        select id from contenedor c
        where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1), 0) as fk_servicio

        FROM public.despachos d
        LEFT JOIN public.clientes c
            ON d.fk_cliente = c.id
        LEFT JOIN public.usuario us
            ON us.id = c.fk_comercial
        INNER JOIN public.notas_cobros nc
            ON CASE
            WHEN nc.fk_despacho = -1 THEN nc.codigo_unificacion=d.codigo_unificacion
            ELSE nc.fk_despacho = d.id END
    WHERE 
    nc.estado<>false
    `+CondicionSelect+`
    `);

    console.log(`
    \nQUERY INSERT \n
    insert into public.master_bd (fecha_creacion, nc_id, n_carpeta, rut, contenedor, id_nave, nombre_nave, eta, m3, monto_din, monto_din_ajuste, monto_carga_usd, monto_carga_clp, monto_carga_clp_ajuste, total_gastos, total_provision, total_a_pagar, monto_pagado, ano, mes, base, aforo, cda, isp, pallets, tvp, otro_transporte, otros, detalle_otro, monto_aju_din, detalle_aju_din, monto_aju_serv, detalle_aju_serv, tc_servicio, precio_base, precio_unitario_x_m3, ejecutivo, mes_eta, din, fk_cliente, din_ingresada_fecha, nombre_cliente, din_pagada_flag, din_pagada_fecha, fk_servicio) 
    SELECT
    DISTINCT
    nc."createdAt" as fecha_creacion
    , nc.id as nc_id
    , coalesce(d.n_carpeta, '') as N_CARPETA
    , coalesce(c.rut, '') as RUT
    , coalesce(d.contenedor, '') as CONTENEDOR
    , coalesce(d.fk_nave, 0) as ID_NAVE
    , coalesce(d.nave_nombre, '[No ingresado]') as NOMBRE_NAVE
    , to_char(d.eta, 'DD-MM-YYYY') as ETA
    , ROUND(nc.carga, 2) as M3
    , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)) as MONTO_DIN
    , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)) as MONTO_DIN_AJUSTE
    , ROUND(coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0), 2) as MONTO_CARGA_USD
    , ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)) as MONTO_CARGA_CLP
    , ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0)) as MONTO_CARGA_CLP_AJUSTE
    , ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0) + coalesce(nc.aforo, 0)+coalesce(nc.isp, 0)+coalesce(nc.cda, 0)+coalesce(nc.almacenaje, 0) + coalesce(nc.pallet*nc.pallet_valor_u, 0)+coalesce(nc.ttvp, 0)+coalesce(nc.twsc, 0)+coalesce(nc.otros, 0)) as TOTAL_GASTOS
    , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)+(coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0)) as TOTAL_PROVISION
    , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)+(coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0) + coalesce(nc.aforo, 0)+coalesce(nc.isp, 0)+coalesce(nc.cda, 0)+coalesce(nc.almacenaje, 0) + coalesce(nc.pallet*nc.pallet_valor_u, 0)+coalesce(nc.ttvp, 0)+coalesce(nc.twsc, 0)+coalesce(nc.otros, 0)) as TOTAL_A_PAGAR
    , ROUND(coalesce((SELECT SUM(dtl.debit_amt) FROM public.wsc_envio_asientos_detalles dtl INNER JOIN public.wsc_envio_asientos_cabeceras cbc ON dtl.fk_cabecera=cbc.id WHERE cbc.carpeta=d.n_carpeta and cbc.estado<>'false' and dtl.desc_text='BANCO SANTANDER'), 0)) as MONTO_PAGADO
    , SUBSTRING(d.n_carpeta, 2, 2) as ANO
    , SUBSTRING(d.n_carpeta, 4, 2) as MES
    , SUBSTRING(d.n_carpeta, 1, 7) as BASE
    , nc.aforo as AFORO
    , nc.cda as CDA
    , nc.isp as ISP
    , coalesce(nc.pallet, 0)*coalesce(nc.pallet_valor_u, 0) as PALLETS
    , coalesce(nc.ttvp, 0) as TVP
    , coalesce(nc.twsc, 0) as OTRO_TRANSPORTE
    , coalesce(nc.otros, 0) as otros
    , coalesce(nc.detalle_otro, '') as detalle_otro
    , coalesce(nc.ajuste_m_1, 0) as MONTO_AJU_DIN
    , coalesce(nc.ajuste_c_1, '') as DETALLE_AJU_DIN
    , coalesce(nc.ajuste_m_2, 0) as MONTO_AJU_SERV
    , coalesce(nc.ajuste_c_2, '') as DETALLE_AJU_SERV
    , ROUND(nc.tc2, 2) as TC_SERVICIO
    , nc.pb as PRECIO_BASE
    , nc.costo as PRECIO_UNITARIO_X_M3
    , coalesce(us.nombre, '') as EJECUTIVO
    , to_char(d.eta, 'MM') as MES_ETA
    , coalesce(nc.din, 0) as din
    , d.fk_cliente
    , coalesce(to_char(nc.din_ingresada_fecha, 'DD-MM-YYYY'), '') as din_ingresada_fecha
    , coalesce(c.codigo, '') as nombre_cliente
    ,
    CASE WHEN nc.din_pagada_flag=true
        THEN 'SI'
        ELSE 'NO'
    END as din_pagada_flag,
    CASE WHEN nc.din_pagada_flag=true
        THEN to_char(nc.din_pagada_fecha, 'DD-MM-YYYY')
        ELSE ''
    END as din_pagada_fecha
    ,
    coalesce((select ct.fk_consolidado
        from tracking t
        inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
        where fk_contenedor = (
        select id from contenedor c
        where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1), 0) as fk_servicio

        FROM public.despachos d
        LEFT JOIN public.clientes c
            ON d.fk_cliente = c.id
        LEFT JOIN public.usuario us
            ON us.id = c.fk_comercial
        INNER JOIN public.notas_cobros nc
            ON CASE
            WHEN nc.fk_despacho = -1 THEN nc.codigo_unificacion=d.codigo_unificacion
            ELSE nc.fk_despacho = d.id END
    WHERE 
    nc.estado<>false
    `+CondicionSelect+`
    `);

    console.log('\n FIN CALCULO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));

    var fecha_carga = moment().format("DD-MM-YYYY HH:mm");

    console.log(` update public.master_bd set ejecucion_fecha='`+fecha_carga+`', link_descarga='`+process.env.UrlRemoteServer1+`get_master_bd' `);
    await client.query(` update public.master_bd set ejecucion_fecha='`+fecha_carga+`', link_descarga='`+process.env.UrlRemoteServer1+`get_master_bd' `);

    res.status(200).send({
        message: "OK",
        success:false,
    }); res.end(); res.connection.destroy();

} catch (error) {
    console.log("ERROR "+error);
    res.status(400).send({
    message: "ERROR AL CARGAR MASTERBD ",
    success:false, }); res.end(); res.connection.destroy();
} };
/************************************************************/
/************************************************************/
exports.GetMasterBd = async (req,res) =>{
    try {

        var Reporte = await client.query(` SELECT fecha_creacion, nc_id, n_carpeta, rut, contenedor, id_nave, nombre_nave, eta, m3, monto_din, monto_din_ajuste, monto_carga_usd, monto_carga_clp, monto_carga_clp_ajuste, total_gastos, total_provision, total_a_pagar, monto_pagado, ano, mes, base, aforo, cda, isp, pallets, tvp, otro_transporte, otros, detalle_otro, monto_aju_din, detalle_aju_din, monto_aju_serv, detalle_aju_serv, tc_servicio, precio_base, precio_unitario_x_m3, ejecutivo, mes_eta, din, fk_cliente, din_ingresada_fecha, nombre_cliente, din_pagada_flag, din_pagada_fecha, fk_servicio FROM public.master_bd `);
        
        var xl = require('excel4node');
    
        var wb = new xl.Workbook();
        
        var hoja_1 = wb.addWorksheet('Master_Bd');
        
        var ajustar_texto = wb.createStyle({
            alignment: { 
                shrinkToFit: true,
                wrapText: true
            },
        });
        
        hoja_1.column(1).setWidth(15);
        hoja_1.column(2).setWidth(15);
        hoja_1.column(3).setWidth(15);
        hoja_1.column(4).setWidth(15);
        hoja_1.column(5).setWidth(15);
        hoja_1.column(6).setWidth(15);
        hoja_1.column(7).setWidth(15);
        hoja_1.column(8).setWidth(15);
        hoja_1.column(9).setWidth(15);
        hoja_1.column(10).setWidth(15);
        hoja_1.column(11).setWidth(15);
        hoja_1.column(12).setWidth(15);
        hoja_1.column(13).setWidth(15);
        hoja_1.column(14).setWidth(15);
        hoja_1.column(15).setWidth(15);
        hoja_1.column(16).setWidth(15);
        hoja_1.column(17).setWidth(15);
        hoja_1.column(18).setWidth(15);
        hoja_1.column(19).setWidth(15);
        hoja_1.column(20).setWidth(15);
        hoja_1.column(21).setWidth(15);
        hoja_1.column(22).setWidth(15);
        hoja_1.column(23).setWidth(15);
        hoja_1.column(24).setWidth(15);
        hoja_1.column(25).setWidth(15);
        hoja_1.column(26).setWidth(15);
        hoja_1.column(27).setWidth(15);
        hoja_1.column(28).setWidth(15);
        hoja_1.column(29).setWidth(15);
        hoja_1.column(30).setWidth(15);
        hoja_1.column(31).setWidth(15);
        hoja_1.column(32).setWidth(15);
        hoja_1.column(33).setWidth(15);
        hoja_1.column(34).setWidth(15);
        hoja_1.column(35).setWidth(15);
        hoja_1.column(36).setWidth(15);
        hoja_1.column(37).setWidth(15);
        hoja_1.column(38).setWidth(15);
        hoja_1.column(39).setWidth(15);
        hoja_1.column(40).setWidth(15);
        hoja_1.column(41).setWidth(15);
        hoja_1.column(42).setWidth(15);
        hoja_1.column(43).setWidth(15);
        hoja_1.column(44).setWidth(15);
        hoja_1.column(45).setWidth(15);
        hoja_1.column(46).setWidth(15);
        hoja_1.column(47).setWidth(15);

        var celda_izquierda = wb.createStyle({
            border: {
                left: {
                    style: 'thin',
                    color: 'black',
                },
                top: {
                    style: 'thin',
                    color: 'black',
                },
                bottom: {
                    style: 'thin',
                    color: 'black',
                },
                outline: false,
            },
        });

        var celda_medio = wb.createStyle({
            border: {
                top: {
                    style: 'thin',
                    color: 'black',
                },
                bottom: {
                    style: 'thin',
                    color: 'black',
                },
                outline: false,
            },
            
        });            

        var celda_derecha = wb.createStyle({
            border: {
                right: {
                    style: 'thin',
                    color: 'black',
                },
                top: {
                    style: 'thin',
                    color: 'black',
                },
                bottom: {
                    style: 'thin',
                    color: 'black',
                },
                outline: false,
            },
        });

        var estilo_titulo = wb.createStyle({
        font: {
            color: '#000000',
            size: 20,
        },
        numberFormat: '$#,##0.00; ($#,##0.00); -',
        });

        var estilo_cabecera = wb.createStyle({
        font: {
            color: '#000000',
            size: 15,
        },
        fill: {
            type: "pattern",
            patternType: "solid",
            bgColor: "#F0F1F2",
            fgColor: "#F0F1F2"
        },
        numberFormat: '$#,##0.00; ($#,##0.00); -',
        });

        var estilo_contenido_texto = wb.createStyle({
        font: {
            color: '#000000',
            size: 12,
        },
        numberFormat: '$#,##0.00; ($#,##0.00); -',
        }); 
        
        var estilo_contenido_numero = wb.createStyle({
            font: {
                color: '#000000',
                size: 12,
            },
        });
        
        try {

            hoja_1.cell(1, 1).string('N° CARPETA').style(estilo_cabecera).style(celda_izquierda);
            hoja_1.cell(1, 2).string('RUT').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 3).string('CONTENEDOR').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 4).string('ID NAVE').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 5).string('NOMBRE NAVE').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 6).string('ETA').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 7).string('M3').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 8).string('MONTO DIN').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 9).string('MONTO DIN AJUSTE').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 10).string('MONTO CARGA USD').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 11).string('MONTO CARGA CLP').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 12).string('MONTO CARGA CLP AJUSTE').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 13).string('TOTAL GASTOS').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 14).string('TOTAL PROVISION').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 15).string('TOTAL A PAGAR').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 16).string('MONTO PAGADO').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 17).string('AÑO').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 18).string('MES').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 19).string('BASE').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 20).string('AFORO').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 21).string('CDA').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 22).string('ISP').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 23).string('PALLETS').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 24).string('TVP').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 25).string('OTRO TRANSPORTE').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 26).string('OTROS').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 27).string('DETALLE OTROS').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 28).string('MONTO AJUSTE DIN').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 29).string('DETALLE AJUSTE DIN').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 30).string('MONTO AJUSTE SERV').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 31).string('DETALLE AJUSTE SERV').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 32).string('TC SERVICIO').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 33).string('PRECIO BASE').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 34).string('PRECIO UNITARIO XM3').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 35).string('EJECUTIVO').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 36).string('MES ETA').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 37).string('N DIN').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 38).string('ID CLIENTE').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 39).string('CODIGO CLIENTE').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 40).string('DIN PAGADA TESORERIA').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 41).string('FECHA PAGO DIN').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 42).string('FECHA INGRESO DIN').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1, 43).string('ID SERVICIO').style(estilo_cabecera).style(celda_medio);
            
            var row = 1;
            for(var i=0; i<Reporte.rows.length; i++)
            {
                row++;
                hoja_1.cell(row, 1).string(Reporte.rows[i]['n_carpeta'].toString()).style(estilo_contenido_texto).style(celda_izquierda);
                hoja_1.cell(row, 2).string(Reporte.rows[i]['rut'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 3).string(Reporte.rows[i]['contenedor'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 4).number(parseInt(Reporte.rows[i]['id_nave'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 5).string(Reporte.rows[i]['nombre_nave'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 6).string(Reporte.rows[i]['eta'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 7).number(parseFloat(Reporte.rows[i]['m3'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 8).number(parseInt(Reporte.rows[i]['monto_din'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 9).number(parseInt(Reporte.rows[i]['monto_din_ajuste'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 10).number(parseFloat(Reporte.rows[i]['monto_carga_usd'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 11).number(parseInt(Reporte.rows[i]['monto_carga_clp'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 12).number(parseInt(Reporte.rows[i]['monto_carga_clp_ajuste'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 13).number(parseInt(Reporte.rows[i]['total_gastos'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 14).number(parseInt(Reporte.rows[i]['total_provision'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 15).number(parseInt(Reporte.rows[i]['total_a_pagar'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 16).number(parseInt(Reporte.rows[i]['monto_pagado'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 17).string(Reporte.rows[i]['ano'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 18).string(Reporte.rows[i]['mes'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 19).string(Reporte.rows[i]['base'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 20).number(parseInt(Reporte.rows[i]['aforo'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 21).number(parseInt(Reporte.rows[i]['cda'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 22).number(parseInt(Reporte.rows[i]['isp'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 23).number(parseInt(Reporte.rows[i]['pallets'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 24).number(parseInt(Reporte.rows[i]['tvp'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 25).number(parseInt(Reporte.rows[i]['otro_transporte'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 26).number(parseInt(Reporte.rows[i]['otros'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 27).string(Reporte.rows[i]['detalle_otro'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 28).number(parseInt(Reporte.rows[i]['monto_aju_din'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 29).string(Reporte.rows[i]['detalle_aju_din'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 30).number(parseInt(Reporte.rows[i]['monto_aju_serv'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 31).string(Reporte.rows[i]['detalle_aju_serv'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 32).number(parseFloat(Reporte.rows[i]['tc_servicio'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 33).number(parseFloat(Reporte.rows[i]['precio_base'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 34).number(parseFloat(Reporte.rows[i]['precio_unitario_x_m3'])).style(estilo_contenido_numero).style(celda_medio);
                hoja_1.cell(row, 35).string(Reporte.rows[i]['ejecutivo'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 36).string(Reporte.rows[i]['mes_eta'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 37).string(Reporte.rows[i]['din'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 38).string(Reporte.rows[i]['fk_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 39).string(Reporte.rows[i]['nombre_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 40).string(Reporte.rows[i]['din_pagada_flag'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 41).string(Reporte.rows[i]['din_pagada_fecha'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 42).string(Reporte.rows[i]['din_ingresada_fecha'].toString()).style(estilo_contenido_texto).style(celda_medio);
                hoja_1.cell(row, 43).number(parseFloat(Reporte.rows[i]['fk_servicio'])).style(estilo_contenido_numero).style(celda_medio);
                
            }
            
        } catch (error) {
            res.send(" 0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0- error hoja 1 "+error);
        }

        wb.write('Reporte_Master_BD.xlsx', res);

    } catch (error) {
        console.log("ERROR "+error);
        res.status(400).send({
        message: "ERROR AL CARGAR MASTERBD ",
        success:false, }); res.end(); res.connection.destroy();
    } };

    /************************************************************/
    /************************************************************/