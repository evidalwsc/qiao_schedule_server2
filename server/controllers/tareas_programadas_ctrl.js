const enviarEmail = require('../../handlers/email');
const cryptoJs = require('crypto-js');
const client = require('../config/db.client');
const clientExp = require('../config/db.client.experienciadigital');
const moment=require('moment');
const QRCode = require("qrcode");
const PDF = require('pdfkit');//Importando la libreria de PDFkit
const fs = require('fs');
const { verify } = require('crypto');
const zoho = require('../../apis/zoho_crm');
const path = require('path');
const funcionesCompartidasCtrl = require('./funcionesCompartidasCtrl.js');
const PDFDocument = require('pdfkit');
const axios = require('axios');
//const ftpConfig = require('../config/ftp');
//const jsftp = require("jsftp");
const { json } = require('body-parser');

const encryptText = async (texto) => {
    //const secret = 'd6F3Efeq';
    const SECRET = process.env.SECRET;
    let hash = cryptoJs.AES.encrypt(texto.toString(), SECRET).toString();
    hash = new Buffer.from(hash).toString("base64");
    //let hash = Math.floor((Math.random() * 9999999999999)+100000000000).toString().slice(0,12)+texto;
    return hash;
}

exports.mail_envios_server2 = async (req, resp) => {
    console.log(".::.");
    console.log(".::.");
    console.log("CONSULTANDO CORREOS 2 ");
    var sche_mail_envios_server2 = require('node-schedule');

    sche_mail_envios_server2.scheduleJob('*/15 * * * * *', () => {
        console.log("ENTRO EN EL CRON JOB ");
        mail_envios_server2();

    });

    async function mail_envios_server2()
    {
        console.log(`
        \n\n
        SELECT
        *
        , 
        case when tipo='mail_notificacion_pago' then coalesce(datos_adicionales,'{}')::jsonb
        else '{}' end as datos_pagos
        FROM
        public.email_envios_logs
        where
        estado='PENDIENTE'
        and id>8900
        order by id
        asc limit 1
        `);

        var Correo = await client.query(`
        SELECT
        *
        , 
        case when tipo='mail_notificacion_pago' then coalesce(datos_adicionales,'{}')::jsonb
        else '{}' end as datos_pagos
        FROM
        public.email_envios_logs
        where
        estado='PENDIENTE'
        and id>8900
        order by id
        asc limit 1
        `);
        var email = '-.com'
        function validateEmail(email)
        {
            var re = /\S+@\S+\.\S+/;
            return re.test(email);
        }

        var EstadoCorreo = null;
        
        if(Correo.rows.length>0)
        {
            console.log("tipo "+Correo.rows[0]['tipo']);
            console.log("tipo_id "+Correo.rows[0]['tipo_id']);
            console.log(".::.");
            console.log(".::.");
            console.log("\n\n\nENVIANDO ID "+Correo.rows[0]['id']);
            console.log("\n\n\nENVIANDO TIPO "+Correo.rows[0]['tipo_id']);
            
            if( Correo.rows[0]['tipo']=='mail_nuevo_usuario' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                await enviarEmail.mail_nuevo_usuario({
                    nombre:Correo.rows[0]['nombre']
                    , apellido:Correo.rows[0]['datos']
                    , email:Correo.rows[0]['para']
                    , usuario:Correo.rows[0]['datos_adicionales']
                    , telefono:Correo.rows[0]['enlace']
                    , asunto:Correo.rows[0]['asunto']
                    , contrasenia:Correo.rows[0]['texto']
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_planificacion_confirmada' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_planificacion_confirmada({
                    asunto:Correo.rows[0]['asunto'],
                    fecha_descarga:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    contenedor:Correo.rows[0]['datos'],
                    clientes:JSON.parse(Correo.rows[0]['datos_adicionales'])
                 });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_contenedor_proforma' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_contenedor_proforma({
                    asunto:Correo.rows[0]['asunto'],
                    texto:Correo.rows[0]['texto'],
                    fecha:Correo.rows[0]['fecha'],
                    servicios:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    emails:JSON.parse(Correo.rows[0]['para']),
                    tracking_id:Correo.rows[0]['tracking_encrypt']
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_tarifa' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_tarifa({
                    asunto:Correo.rows[0]['asunto'],
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    nombre:Correo.rows[0]['nombre'],
                 });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_question' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_question({
                    asunto:Correo.rows[0]['asunto'],
                    name:Correo.rows[0]['nombre'],
                    linksQuestion:Correo.rows[0]['enlace'],
                    to:Correo.rows[0]['para'],
                    datos: JSON.parse(Correo.rows[0]['datos']),
                    comercial:JSON.parse(Correo.rows[0]['comercial'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_6' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_6({
                    asunto:Correo.rows[0]['asunto'],
                    nombreUsuario:Correo.rows[0]['nombre'],
                    fecha:Correo.rows[0]['fecha'],
                    host:Correo.rows[0]['enlace'],
                    tipo:Correo.rows[0]['tipo_id'],
                    email:Correo.rows[0]['para'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:Correo.rows[0]['tracking_encrypt']
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_1' )
            {
                if( Correo.rows[0]['tipo_id']=='16' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='17' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='18' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='14' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        texto:JSON.parse(Correo.rows[0]['texto']),
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='22' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='23' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='19' || Correo.rows[0]['tipo_id']=='20')
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        texto:JSON.parse(Correo.rows[0]['texto']),
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='99' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        texto:JSON.parse(Correo.rows[0]['texto']),
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='100' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else
                {
                    
                    if( Correo.rows[0]['adjunto']=='' || Correo.rows[0]['adjunto']==null )
                    {
                        await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                        EstadoCorreo = await enviarEmail.mail_notificacion_1({
                            asunto:Correo.rows[0]['asunto'],
                            nombreUsuario:Correo.rows[0]['nombre'],
                            texto:JSON.parse(Correo.rows[0]['texto']),
                            fecha:Correo.rows[0]['fecha'],
                            email:Correo.rows[0]['para'],
                            comercial:JSON.parse(Correo.rows[0]['comercial']),
                            tracking_id:Correo.rows[0]['tracking_encrypt'],
                            host:Correo.rows[0]['enlace'],
                        });
                        
                    }
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_pago' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);

                EstadoCorreo = await enviarEmail.mail_notificacion_pago({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:JSON.parse(Correo.rows[0]['datos_pagos']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_recepcion' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_recepcion({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:JSON.parse(Correo.rows[0]['datos']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:Correo.rows[0]['tracking_encrypt']
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_retiro_programado' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_retiro_programado({
                    asunto:Correo.rows[0]['asunto'],
                    //cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    //datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    //url:Correo.rows[0]['enlace'],
                    comercial:JSON.parse(Correo.rows[0]['email_comercial']),
                    //comercial:JSON.parse(Correo.rows[0]['comercial']),
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_consolidacion_rapida' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_consolidacion_rapida({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:null
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            /*else if( Correo.rows[0]['tipo']=='mail_notificacion_consolidacion_seleccionable' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                let datosAdicionales=JSON.parse(Correo.rows[0]['datos_adicionales']);
                let str='';let str2='';
                if(datosAdicionales.borrados && datosAdicionales.borrados.length){
                    for(let i=0;i<datosAdicionales.borrados.length;i++){
                        if(i<datosAdicionales.borrados.length-1){
                            str+=datosAdicionales.borrados[i]+' - ';
                        }else if(i==datosAdicionales.borrados.length-1){
                            str+=datosAdicionales.borrados[i];
                        }
                    }
                }

                if(datosAdicionales.modificados && datosAdicionales.modificados.length){
                    for(let i=0;i<datosAdicionales.modificados.length;i++){
                        if(i<datosAdicionales.modificados.length-1){
                            str2+=datosAdicionales.modificados[i]+'-';
                        }else if(i==datosAdicionales.modificados.length-1){
                            str2+=datosAdicionales.modificados[i];
                        }
                    }
                }

                datosAdicionales.deletes=str;
                datosAdicionales.modify='';
                if(str2.length>0){
                    datosAdicionales.modify=await encryptText(str2);
                }
                
                EstadoCorreo = await enviarEmail.mail_notificacion_new_consolidado({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:datosAdicionales,
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:null,
                    attachments: JSON.parse(Correo.rows[0]['adjunto'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }*/
            else if( Correo.rows[0]['tipo']=='mail_notificacion_confirmacion_consolidacion_rapida' )
            {
                console.log('\n\n PROCESANDO mail_notificacion_confirmacion_consolidacion_rapida')
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_confirmacion_consolidacion_rapida({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:null
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
        }

        async function ActualizarEstadoEnvioCorreo(id, estado)
        {
            if(EstadoCorreo==true)
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIADO' where id=`+id+` `);
            }
            else if(EstadoCorreo==false)
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ERROR' where id=`+id+` `);
            }
            else
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='INDEFINIDO' where id=`+id+` `);
            }
        }
    }
}

exports.ProcesarExcelGatillos = async (req, res) => {
    console.log(".::.");
    console.log(".::.");
    const moment = require('moment');
        console.log(moment().format("DD-MM-YYYY HH:mm")+'\n\n');
    var shc_ProcesarExcelGatillos = require('node-schedule');

    shc_ProcesarExcelGatillos.scheduleJob('*/30 * * * * *', () => {
        function_ProcesarExcelGatillos();

    });

    async function function_ProcesarExcelGatillos(){

        var ExistenDatos = await client.query(` select * FROM public.excel_despachos WHERE mensaje_carga='01.- ARCHIVO CARGADO' `);

        if(ExistenDatos && ExistenDatos.rows && ExistenDatos.rows.length>0)
        {

            await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n02.- PROCESANDO DATOS PARA REPORTE') `);

            await client.query(` DELETE FROM public.sla_00_completo where estado_despacho!='ENTREGADO' `);

            console.log('.::.');
            console.log('.::.');
            console.log('.::.');
            console.log('Insertando el reporte ');
            await client.query(`
            INSERT INTO public.sla_00_completo(id_proveedor, nombre_proveedor, fecha_creacion_proveedor, id_cliente, razon_social_cliente, ejecutivo, bultos_esperados, m3_esperados, peso_esperado, bultos_recepcionados, bodega_recepcion, fecha_ultima_carga_documentos, fecha_ultima_recepcion, fecha_de_creacion_del_consolidado, fecha_cierre_consolidado_comercial, id_consolidado_comercial, tracking_id, proforma_id, fecha_consolidado_contenedor, fecha_ingreso_datos_contenedor_nave_eta, n_contenedor, despacho_id, nombre_nave, etd_nave_asignada, fecha_nueva_etd_o_eta, eta, n_carpeta, fecha_publicacion, aforo, fecha_aforo, fecha_retiro, hora_retiro, fecha_desconsolidacion_pudahuel, hora_desconsolidacion, estado_finanzas, fecha_de_pago, fecha_ingreso_direccion, fecha_programada, fecha_entrega_retiro, estado_despacho, dias_libres, fecha_creacion_cliente, m3_recibidos) 
            select

            coalesce(prov.id::text, ''::text) AS id_proveedor

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (prov.nombre IS NULL OR prov.nombre='' ) then 'OK' else 
            coalesce(prov.nombre, ''::character varying)
            end as nombre_proveedor

            , case when to_char(tck.fecha_creacion, 'YYYY/MM/DD'::text)='00/01/1900' or tck.fecha_creacion is null then '01/01/2000' else 
            to_char(tck.fecha_creacion, 'DD/MM/YYYY'::text)
            end as fecha_creacion_proveedor

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cli.id IS NULL ) then '0000' else 
            coalesce(cli.id::text, ''::text)
            end as id_cliente

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cli."razonSocial" IS NULL or cli."razonSocial"='' ) then 'OK' else 
            coalesce(cli."razonSocial", ''::character varying)
            end as razon_social_cliente

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (comer.nombre IS NULL or comer.nombre='' ) then 'OK' else 
            coalesce(comer.nombre, ''::character varying)
            end as ejecutivo

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.cantidad_bultos IS NULL ) then '0000'::text else 
            coalesce(tck.cantidad_bultos::text, ''::text)
            end as bultos_esperados

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.volumen IS NULL ) then '0000' else 
            REPLACE(coalesce(tck.volumen::text,''), '.', ',')
            end as m3_esperado

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.peso IS NULL ) then '0000' else 
            REPLACE(coalesce(tck.peso::text,''), '.', ',')
            end as peso_esperado

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (SUM(tckd.cantidad) IS NULL ) then '0000' else 
            SUM(tckd.cantidad)::text
            end as bultos_recepcionados

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (bod.nombre IS NULL or bod.nombre='') then 'OK' else 
            bod.nombre
            end as bodega_recepcion

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.fecha_ultima_carga_documento IS NULL) then '01/01/2022' else 
            coalesce(to_char(tck.fecha_ultima_carga_documento, 'DD/MM/YYYY'::text),'')
            end as fecha_ultima_carga_documentos

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.fecha_ultima_recepcion IS NULL or tck.fecha_ultima_recepcion::text='') then '2000/01/01' else 
            coalesce(tck.fecha_ultima_recepcion::text,'')
            end as fecha_ultima_recepcion

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cons.fecha IS NULL) then '01/01/2000' else 
            to_char(cons.fecha, 'DD/MM/YYYY'::text)
            end as fecha_de_creacion_del_consolidado

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cons.fecha_cierre IS NULL) then '01/01/2000' else 
            to_char(cons.fecha_cierre, 'DD/MM/YYYY'::text)
            end as fecha_cierre_consolidado_comercial

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cons.id IS NULL) then '0000' else 
            COALESCE(cons.id::text, ''::text)
            end as id_consolidado_comercial

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.id IS NULL) then '0000' else 
            COALESCE(tck.id::text, ''::text)
            end as tracking_id

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.fk_proforma IS NULL) then '0000' else 
            COALESCE(tck.fk_proforma::text, ''::text)
            end as proforma_id

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (prof.fecha_consolidado IS NULL) then '01/01/2000' else 
            to_char(prof.fecha_consolidado, 'DD/MM/YYYY'::text)
            end as fecha_consolidado_contenedor

            , '' as fecha_ingreso_datos_contenedor_nave_eta

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.fk_contenedor_codigo IS NULL or tck.fk_contenedor_codigo='') then 'OK' else 
            COALESCE(tck.fk_contenedor_codigo::text, ''::text)
            end as n_contenedor

            , '0000' as despacho_id

            , (
            SELECT n2.nave_nombre FROM naves2 n2
            LEFT JOIN naves_eta ne on ne.fk_nave=n2.nave_id
            LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id
            LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking
            where ct.id=prof.fk_contenedor_tracking and ne.estado<2 order by ne.id asc limit 1
            ) as nombre_nave

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') )
            and (SELECT ne.etd_fecha FROM naves_eta ne LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking where ct.id=prof.fk_contenedor_tracking order by ne.id asc limit 1) is null then '01/01/2000' 
            else (SELECT to_char(ne.etd_fecha, 'DD/MM/YYYY'::text) FROM naves_eta ne LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking where ct.id=prof.fk_contenedor_tracking order by ne.id asc limit 1)
            end
            as etd_nave_asignada

            , '' fecha_nueva_etd_o_eta

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) 
            and (SELECT ne.eta_fecha FROM naves_eta ne LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking where ct.id=prof.fk_contenedor_tracking order by ne.id desc limit 1) is null then '01/01/2000' 
            else (SELECT to_char(ne.eta_fecha, 'DD/MM/YYYY'::text) FROM naves_eta ne LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking where ct.id=prof.fk_contenedor_tracking order by ne.id desc limit 1) 
            end
            as eta

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (desp.n_carpeta IS NULL or desp.n_carpeta='') then 'OK' else 
            COALESCE(desp.n_carpeta::text, ''::text)
            end as n_carpeta

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.listado_aforo IS NULL or exc.listado_aforo='') then '01/01/2000' else 
            COALESCE(exc.listado_aforo, ''::text)
            end as fecha_publicacion

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.aforo IS NULL or exc.aforo='') then 'OK' else 
            COALESCE(exc.aforo, ''::text)
            end as aforo

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.fecha_aforo IS NULL or exc.fecha_aforo='') then '01/01/2022' else 
            COALESCE(exc.fecha_aforo, ''::text)
            end as fecha_aforo

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.eta_tnm IS NULL or exc.eta_tnm='') then '01/01/2022' else 
            COALESCE(exc.eta_tnm, ''::text)
            end as fecha_retiro

            , '' as hora_retiro

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.eta_bodega IS NULL or exc.eta_bodega='') then '01/01/2022' else 
            COALESCE(exc.eta_bodega, ''::text)
            end as fecha_desconsolidacion_pudahuel

            , '' as hora_desconsolidacoin

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.pago_finanzas IS NULL or exc.pago_finanzas='') then 'OK' else 
            COALESCE(exc.pago_finanzas, ''::text)
            end as estado_finanzas

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.ultimo_pago IS NULL or exc.ultimo_pago='') then '01/01/2022' else 
            COALESCE(exc.ultimo_pago, ''::text)
            end as fecha_de_pago

            , '' as fecha_ingreso_direccion

            , '' as fecha_programada

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) 
			and (select temp1.fecha_entrega from public.tracking_entrega as temp1 where temp1.fk_tracking=tck.id and temp1.fecha_entrega is not null and temp1.estado is true order by id desc limit 1) is null
			and (exc.eta_despacho IS NULL or exc.eta_despacho='') then '01/01/2022' 
			when (select temp1.fecha_entrega from public.tracking_entrega as temp1 where temp1.fk_tracking=tck.id and temp1.fecha_entrega is not null and temp1.estado is true order by id desc limit 1) is not null then 
			to_char((select temp1.fecha_entrega from public.tracking_entrega as temp1 where temp1.fk_tracking=tck.id and temp1.fecha_entrega is not null and temp1.estado is true order by id desc limit 1), 'DD/MM/YYYY'::text)
			else
            COALESCE(exc.eta_despacho, ''::text)
            end as fecha_entrega_retiro

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.estado_despacho IS NULL or exc.estado_despacho='') then 'OK' else 
            COALESCE(exc.estado_despacho, ''::text)
            end as estado_despacho

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.dias_libres IS NULL or exc.dias_libres='') then '0000' else 
            COALESCE(exc.dias_libres, ''::text)
            end as dias_libres

            , case when to_char(to_date(cli."fechaCreacion", 'YYYY/MM/DD'), 'YYYY/MM/DD'::text)='00/01/1900' then '01/01/2000' else to_char(to_date(cli."fechaCreacion", 'YYYY/MM/DD'), 'DD/MM/YYYY'::text) end as fecha_creacion_cliente

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.volumen_recepcionado IS NULL ) then '0000' else 
            REPLACE(coalesce(tck.volumen_recepcionado::text,''), '.', ',')
            end as m3_recibidos

            from
            public.tracking as tck
            inner join public.clientes as cli on cli.id = tck.fk_cliente and cli.valido_reportes='SI'

            left join public.tracking_detalle_agrupado as tckd on tckd.tracking_id=tck.id
            left join public.usuario as comer on cli.fk_comercial = comer.id
            left join public.proveedores as prov on prov.id = tck.fk_proveedor
            left join public.bodegas as bod on bod.id = tck.fk_bodega
            left join public.consolidado_tracking constck on constck.fk_tracking = tck.id
            left join public.consolidado as cons on constck.fk_consolidado = cons.id
            left join public.contenedor_proforma as prof on tck.fk_proforma = prof.id

            left join public.despachos as desp on desp.id = ( SELECT temp1.id FROM despachos temp1 WHERE 
            UPPER(regexp_replace(temp1.contenedor,'\\s','','g'))::text=UPPER(regexp_replace(tck.fk_contenedor_codigo,'\\s','','g'))::text
            AND tck.fk_cliente::text=temp1.fk_cliente::text  ORDER BY temp1.id DESC LIMIT 1)
            left join public.excel_despachos as exc ON 
            tck.fk_cliente::text = UPPER(regexp_replace(exc.id_cliente,'\\s','','g'))::text
            AND UPPER(regexp_replace(desp.n_carpeta,'\\s','','g'))::text = UPPER(regexp_replace(exc.carpeta,'\\s','','g'))::text 
            AND UPPER(regexp_replace(tck.fk_contenedor_codigo,'\\s','','g'))::text = UPPER(regexp_replace(exc.contenedor,'\\s','','g'))::text

            where
            tck.estado_sla_00 is not true 
            and tck.estado>=0 

            group by 
            prov.id
            , tck.fecha_creacion
            , cli.id
            , comer.nombre
            , tck.cantidad_bultos
            , tck.volumen
            , tck.volumen_recepcionado
            , tck.peso
            , bod.nombre
            , tck.fecha_ultima_carga_documento
            , tck.fecha_ultima_recepcion
            , cons.fecha
            , cons.fecha_cierre
            , cons.id
            , tck.id
            , prof.fecha_consolidado
            , prof.fk_contenedor_tracking

            , desp.n_carpeta
            , exc.listado_aforo
            , exc.aforo
            , exc.fecha_aforo
            , exc.eta_tnm
            , exc.eta_bodega
            , exc.pago_finanzas
            , exc.ultimo_pago
            , exc.eta_despacho
            , exc.estado_despacho
            , exc.dias_libres
            `);

            await client.query(`
            UPDATE tracking a SET estado_sla_00 = true WHERE estado_sla_00 is not true and EXISTS (SELECT FROM sla_00_completo b WHERE b.tracking_id::text = a.id::text and b.estado_despacho='ENTREGADO')
            `);

            await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n03.- CAPTURADO DATOS PARA ARCHIVO DE REPORTE') `);

            console.log('.::.');
            console.log('.::.');
            console.log('.::.');
            console.log('Capturando el porte');
            var Reporte = await client.query(`
            SELECT
    coalesce(s.id_proveedor,'') as id_proveedor
    , coalesce(s.nombre_proveedor,'') as nombre_proveedor

    , case when length(s.fecha_creacion_proveedor)>0 and s.fecha_creacion_proveedor!='' and s.fecha_creacion_proveedor is not null and s.fecha_creacion_proveedor!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_creacion_proveedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_creacion_proveedor

    , coalesce(s.id_cliente,'') as id_cliente
    , coalesce(s.razon_social_cliente,'') as razon_social_cliente
    , coalesce(s.ejecutivo,'') as ejecutivo_comercial
    ,CASE
      WHEN c.fk_ejecutivocuenta IS NOT NULL THEN
      coalesce(CONCAT(u.nombre,' ',u.apellidos),'')
      ELSE
      '' 
      END as ejecutivo_cuenta
    , coalesce(s.bultos_esperados::text,'') as bultos_esperados
    , REPLACE(coalesce(s.m3_esperados::text,''), '.', ',') as m3_esperados
    , REPLACE(coalesce(s.m3_recibidos::text,''), '.', ',') as m3_recibidos
    , coalesce(s.peso_esperado::text,'') as peso_esperado
    , coalesce(s.bultos_recepcionados::text,'') as bultos_recepcionados
    , coalesce(s.bodega_recepcion,'') as bodega_recepcion

    , case when s.fecha_ultima_carga_documentos='OK' then s.fecha_ultima_carga_documentos
    when length(s.fecha_ultima_carga_documentos)>0 and s.fecha_ultima_carga_documentos!='' and s.fecha_ultima_carga_documentos is not null and s.fecha_ultima_carga_documentos!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_ultima_carga_documentos, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else

    case when s.fecha_cierre_consolidado_comercial='OK' then s.fecha_cierre_consolidado_comercial
    when length(s.fecha_cierre_consolidado_comercial)>0 and s.fecha_cierre_consolidado_comercial!='' and s.fecha_cierre_consolidado_comercial is not null and s.fecha_cierre_consolidado_comercial!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end

    end as fecha_ultima_carga_documentos

    , coalesce(s.fecha_aprobacion_documentos,'') as fecha_aprobacion_documentos

    , case when s.fecha_ultima_recepcion='OK' then s.fecha_ultima_recepcion
    when length(s.fecha_ultima_recepcion)>0 and s.fecha_ultima_recepcion!='' and s.fecha_ultima_recepcion is not null and s.fecha_ultima_recepcion!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_ultima_recepcion, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
    '' end as fecha_ultima_recepcion_registro_1

    , case when s.fecha_de_creacion_del_consolidado='OK' then s.fecha_de_creacion_del_consolidado
    when length(s.fecha_de_creacion_del_consolidado)>0 and s.fecha_de_creacion_del_consolidado!='' and s.fecha_de_creacion_del_consolidado is not null and s.fecha_de_creacion_del_consolidado!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_de_creacion_del_consolidado, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_de_creacion_del_consolidado

    , case when s.fecha_cierre_consolidado_comercial='OK' then s.fecha_cierre_consolidado_comercial
    when length(s.fecha_cierre_consolidado_comercial)>0 and s.fecha_cierre_consolidado_comercial!='' and s.fecha_cierre_consolidado_comercial is not null and s.fecha_cierre_consolidado_comercial!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_cierre_consolidado_comercial

    , coalesce(s.id_consolidado_comercial,'') as id_consolidado_comercial
    , coalesce(s.tracking_id,'') as tracking_id
    , coalesce(s.proforma_id,'') as proforma_id

    , case when s.fecha_consolidado_contenedor='OK' then s.fecha_consolidado_contenedor
    when length(s.fecha_consolidado_contenedor)>0 and s.fecha_consolidado_contenedor!='' and s.fecha_consolidado_contenedor is not null and s.fecha_consolidado_contenedor!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_consolidado_contenedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_consolidado_contenedor

    , case when s.fecha_ingreso_datos_contenedor_nave_eta='OK' then s.fecha_ingreso_datos_contenedor_nave_eta
    when length(s.fecha_ingreso_datos_contenedor_nave_eta)>0 and s.fecha_ingreso_datos_contenedor_nave_eta!='' and s.fecha_ingreso_datos_contenedor_nave_eta is not null and s.fecha_ingreso_datos_contenedor_nave_eta!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_ingreso_datos_contenedor_nave_eta, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
    '' end as fecha_ingreso_datos_contenedor_nave_eta

    , coalesce(s.n_contenedor,'') as n_contenedor
    , coalesce(s.despacho_id,'') as despacho_id
    , coalesce(s.nombre_nave,'') as nombre_nave

    , case when s.etd_nave_asignada='OK' then s.etd_nave_asignada
    when length(s.etd_nave_asignada)>0 and s.etd_nave_asignada!='' and s.etd_nave_asignada is not null and s.etd_nave_asignada!='#¡REF!' then
    coalesce(to_char(to_date(s.etd_nave_asignada, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as etd_nave_asignada

    , case when s.fecha_nueva_etd_o_eta='OK' then s.fecha_nueva_etd_o_eta
    when length(s.fecha_nueva_etd_o_eta)>0 and s.fecha_nueva_etd_o_eta!='' and s.fecha_nueva_etd_o_eta is not null and s.fecha_nueva_etd_o_eta!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_nueva_etd_o_eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_nueva_etd_o_eta

    , case when s.eta='OK' then s.eta
    when length(s.eta)>0 and s.eta!='' and s.eta is not null then
    coalesce(to_char(to_date(s.eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as eta

    , coalesce(s.n_carpeta,'') as n_carpeta

	, coalesce(s.fecha_retiro_puerto,'') as fecha_retiro_puerto
    , coalesce(s.hora_retiro,'') as hora_retiro

    , case when s.fecha_desconsolidacion_pudahuel='OK' then s.fecha_desconsolidacion_pudahuel
    when length(s.fecha_desconsolidacion_pudahuel)>0 and s.fecha_desconsolidacion_pudahuel!='' and s.fecha_desconsolidacion_pudahuel is not null and s.fecha_desconsolidacion_pudahuel!='#¡REF!'then 
    coalesce(to_char(to_date(s.fecha_desconsolidacion_pudahuel, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_desconsolidacion_pudahuel

    , coalesce(s.hora_desconsolidacion,'') as hora_desconsolidacion
    , coalesce(s.estado_finanzas,'') as estado_finanzas

    , case when s.fecha_de_pago='OK' then s.fecha_de_pago
    when length(s.fecha_de_pago)>0 and s.fecha_de_pago!='' and s.fecha_de_pago is not null and s.fecha_de_pago!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_de_pago, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_de_pago_registro_1

    , case when s.fecha_ingreso_direccion='OK' then s.fecha_ingreso_direccion
    when length(s.fecha_ingreso_direccion)>0 and s.fecha_ingreso_direccion!='' and s.fecha_ingreso_direccion is not null and s.fecha_ingreso_direccion!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_ingreso_direccion, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_ingreso_direccion

    , case when s.fecha_entrega_retiro='OK' then s.fecha_entrega_retiro
    when length(s.fecha_entrega_retiro)>0 and s.fecha_entrega_retiro!='' and s.fecha_entrega_retiro is not null and s.fecha_entrega_retiro!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_entrega_retiro, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_entrega
    , coalesce(s.chofer,'') as chofer
    , coalesce(s.dias_libres,'') as dias_libres

    , coalesce(s.fecha_creacion_cliente,'') as fecha_creacion_cliente
    , case when tdr.fk_registro_direccion is not null then 
	concat(dir.direccion,' ',dir.numero,', ',comunas.nombre,', ',region.nombre) else '' end 
	as fk_direccion_completa
	,case when tdr.fk_registro_direccion is not null then comunas.nombre else '' end as fk_comuna_nombre
    ,region.id as fk_region
	,case when tdr.fk_registro_direccion is not null then region.nombre else '' end as fk_region_nombre
	,case when te.fecha_programada is not null then coalesce(to_char(te.fecha_programada, 'DD/MM/YYYY'),'') else
    '' end as fecha_programada 
	,case when tdr.fecha is not null then coalesce(to_char(tdr.fecha, 'DD/MM/YYYY'),'') else
    '' end as fecha_solicitud_despacho 
	,CASE 
                    WHEN tdr.fk_bodega is not null then 'RETIRO' 
                    WHEN tdr.empresa_ext_retiro is not null then 'RETIRA TRANS.EXTERNO' 
                    WHEN tdr.empresa_ext_despacho is not null then 'DESPACHO TRANS.EXTERNO' 
                    WHEN tdr.fk_registro_direccion is null and tdr.fk_bodega is null and tdr.empresa_ext_retiro is null and tdr.empresa_ext_despacho is null then 'SIN ESPECIFICAR'
                    WHEN tdr.fk_registro_direccion is not null and dir.fk_region!=12 and dir.fk_region is not null then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
                    WHEN tdr.fk_registro_direccion is not null and dir.fk_region=12 and dir.fk_region is not null and dir.fk_comuna in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
                    WHEN tdr.fk_registro_direccion is not null and dir.fk_region=12 and dir.fk_region is not null and dir.fk_comuna not in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'DESPACHO GRATIS INCLUIDO'
                    ELSE 'SIN ESPECIFICAR' END tipo_entrega
	,CASE WHEN te.estado_entrega=1 then 'ENTREGADO' WHEN te.estado_entrega=2 then 'PARCIAL' ELSE '' END as estado_entrega
	,case when t.fk_proforma is not null then
	coalesce(to_char(cp.fecha_prog_aforo, 'DD/MM/YYYY'),'') else
    '' end as fecha_publicacion_aforo 
	, coalesce(s.aforo,'') as aforo
	,coalesce(s.fecha_aforo,'') as fecha_aforo 
	,case when t.fk_proforma is not null then
	coalesce(to_char(to_date(cp.fecha_tnm_retiro,'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_tnm_retiro 
	,case when t.fk_proforma is not null then
	coalesce(to_char(cp.fecha_retiro_puerto, 'DD/MM/YYYY'),'') else
    '' end as fecha_retiro_puerto 
	,CASE
      WHEN te.id IS NOT NULL THEN
      coalesce(CONCAT(u2.nombre,' ',u2.apellidos),'')
      ELSE
      null 
      END as responsable_entrega
	 ,case when nc."m3" is not null then
	nc."m3" else
    0 end as m3
	,case when s.id_consolidado_comercial is not null and s.id_consolidado_comercial!='' then
	coalesce(to_char(d."createdAt", 'DD/MM/YYYY'),'') else
    '' end as fecha_envio_nc
	,case when t.fecha_recepcion_1 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_1, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_1
	,case when t.fecha_recepcion_2 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_2, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_2
	,case when t.fecha_recepcion_3 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_3, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_3
	,case when t.fecha_recepcion_4 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_4, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_4
	,case when t.fecha_recepcion_5 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_5, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_5
	,case when nc.fecha_pago_1 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_1, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_1
	,case when nc.fecha_pago_2 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_2, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_2
	,case when nc.fecha_pago_3 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_3, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_3
	,case when nc.fecha_pago_4 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_4, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_4
	,case when nc.fecha_pago_5 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_5, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_5
	
    FROM public.sla_00_completo s
	INNER JOIN public.clientes c on c.id=s.id_cliente::integer
	LEFT JOIN public.usuario u on u.id=c.fk_ejecutivocuenta
	LEFT JOIN public.tracking_despacho_retiro tdr on tdr.fk_tracking=s.tracking_id::integer
	LEFT JOIN public.clientes_direcciones as dir on dir.id=tdr.fk_registro_direccion
	LEFT JOIN public.comunas on comunas.id=dir.fk_comuna
	LEFT JOIN public.region on region.id=dir.fk_region
	LEFT JOIN public.tracking t on t.id=s.tracking_id::integer
	LEFT JOIN public.contenedor_proforma cp on cp.id=t.fk_proforma
	LEFT JOIN public.tracking_entrega te on te.fk_tracking=s.tracking_id::integer and te.estado=true
	LEFT JOIN public.usuario u2 on u2.id=te.fk_usuario 
	LEFT JOIN public.consolidado cns on cns.id=s.id_consolidado_comercial::integer
	LEFT JOIN public.despachos d on d.fk_cliente=t.fk_cliente and d.fk_proforma=t.fk_proforma and d.estado=true
	LEFT JOIN public.notas_cobros nc on nc.fk_despacho=d.id
            `);

            var xl = require('excel4node');

            var wb = new xl.Workbook();

            var hoja_1 = wb.addWorksheet('Reporte');

            var ajustar_texto = wb.createStyle({
                alignment: { 
                    shrinkToFit: true,
                    wrapText: true
                },
            });

            for(var AuxCol=1; AuxCol<=41; AuxCol++)
            {
                hoja_1.column(AuxCol).setWidth(15);
            }

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

                var row = 1;
                var col = 1;
                hoja_1.cell(1,1).string('id_proveedor').style(estilo_cabecera).style(celda_izquierda);
                hoja_1.cell(1,2).string('nombre_proveedor').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,3).string('fecha_creacion_proveedor').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,4).string('id_cliente').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,5).string('razon_social_cliente').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,6).string('ejecutivo_comercial').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,6).string('ejecutivo_cuenta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,7).string('bultos_esperados').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,8).string('m3_esperados').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,9).string('peso_esperado').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,10).string('bultos_recepcionados').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,11).string('bodega_recepcion').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,12).string('fecha_ultima_carga_documentos').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,13).string('fecha_ultima_recepcion_registro_1').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,13).string('fecha_ultima_recepcion_registro_2').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,13).string('fecha_ultima_recepcion_registro_3').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,13).string('fecha_ultima_recepcion_registro_4').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,13).string('fecha_ultima_recepcion_registro_5').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,14).string('fecha_de_creacion_del_consolidado').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,15).string('fecha_cierre_consolidado_comercial').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,16).string('id_consolidado_comercial').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,17).string('tracking_id').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,18).string('proforma_id').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,19).string('fecha_consolidado_contenedor').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,20).string('fecha_ingreso_datos_contenedor_nave_eta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,21).string('n_contenedor').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,22).string('despacho_id').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,23).string('nombre_nave').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,24).string('etd_nave_asignada').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,25).string('fecha_nueva_etd_o_eta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,26).string('eta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,27).string('n_carpeta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,28).string('fecha_publicacion_aforo').style(estilo_cabecera).style(celda_medio);
               /* hoja_1.cell(1,28).string('fecha_publicacion').style(estilo_cabecera).style(celda_medio);*/
                hoja_1.cell(1,29).string('aforo').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,30).string('fecha_prog_aforo').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,31).string('fecha_retiro_puerto').style(estilo_cabecera).style(celda_medio);
                /*hoja_1.cell(1,31).string('fecha_retiro').style(estilo_cabecera).style(celda_medio);*/
                hoja_1.cell(1,32).string('hora_retiro').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,33).string('fecha_desconsolidacion_pudahuel').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,34).string('hora_desconsolidacion').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,35).string('estado_finanzas').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('fecha_de_pago_registro_1').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('fecha_de_pago_registro_2').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('fecha_de_pago_registro_3').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('fecha_de_pago_registro_4').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('fecha_de_pago_registro_5').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('fecha_solicitud_despacho').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('fecha_prog_despacho').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('direccion_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('comuna').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('tipo_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('fecha_listo_para_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,37).string('fecha_ingreso_direccion').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,38).string('fecha_programada').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,39).string('fecha_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,40).string('estado_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,41).string('dias_libres').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,42).string('fecha_creacion_cliente').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,43).string('m3_recibidos').style(estilo_cabecera).style(celda_derecha);

                
                for(var i=0; i<Reporte.rows.length; i++)
                {
                    row++;
                    col = 1;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_proveedor'].toString()).style(estilo_contenido_texto).style(celda_izquierda); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_proveedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_proveedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['razon_social_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_cuenta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_esperados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_esperados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['peso_esperado'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_recepcionados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['bodega_recepcion'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ultima_carga_documentos'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_recepcion_1'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_recepcion_2'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_recepcion_3'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_recepcion_4'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_recepcion_5'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_de_creacion_del_consolidado'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_cierre_consolidado_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_consolidado_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['tracking_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['proforma_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_consolidado_contenedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ingreso_datos_contenedor_nave_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_contenedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['despacho_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_nave'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['etd_nave_asignada'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_nueva_etd_o_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_carpeta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_publicacion_aforo'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['aforo'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_aforo'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_retiro_puerto'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['hora_retiro'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_desconsolidacion_pudahuel'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['hora_desconsolidacion'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_finanzas'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_pago_1'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_pago_2'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_pago_3'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_pago_4'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_pago_5'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_solicitud_despacho'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_programada'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fk_direccion_completa'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fk_comuna_nombre']==null ? '':''+Reporte.rows[i]['fk_comuna_nombre'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['tipo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ingreso_direccion'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_programada'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['chofer'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['dias_libres'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_recibidos'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                }

            } catch (error) {
                await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n03.- ERROR `+error+`') `);
                console.log("ERROR "+error);
                res.status(400).send({
                    message: "ERROR AL CREAR EXCEL",
                    success:false,
                }); res.end(); res.connection.destroy();
            }

            console.log('.::.');
            console.log('.::.');
            console.log('.::.');
            console.log('Guardando excel ');


            await client.query(` 
            update public.excel_despachos 
            set 
            mensaje_carga = concat(mensaje_carga,'\n\n04.- REPORTE GENERADO') 
            , link_archivo='`+process.env.UrlRemoteServer1+`get_reporte_gatillos'
            `);

            wb.write('./public/files/exceldespachos/reporte_gatillos.xls', async function(err, stats) {
                if (err) 
                {
                    await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n04.- ERROR `+err+`') `);
                    console.log("ERROR "+err);
                } else {
                }
            });
        }

    }
};