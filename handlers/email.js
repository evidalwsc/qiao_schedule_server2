const client = require('../server/config/db.client');
const clientTnm = require('../server/config/db.client.tnm');
const nodemailer = require('nodemailer');
const juice = require('juice');
const htmltoText = require('html-to-text');
const util = require('util');
const pug = require('pug');
const path = require('path');
const lodash = require('lodash');
const emailConfig = require('../config/emails');
const { POINT_CONVERSION_HYBRID } = require('constants');
const console = require('console');
const moment = require('moment');
var funcionesCompartidasCtrl = require('../server/controllers/funcionesCompartidasCtrl.js');

let transport_TNM = nodemailer.createTransport(emailConfig.transport_TNM);

let casillabcc='notificaciones@wscargo.cl';

/**********************************************/
/**********************************************/
/**********************************************/
const view_mail_reporte_clientes = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_reporte_clientes.pug', opciones);
    return juice(html);
}
exports.mail_reporte_clientes = async(opciones) => {
    const html = view_mail_reporte_clientes(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.email,
        subject: opciones.asunto,
        text,
        html,
        attachments: [
        {
            filename: 'Reporte Clientes.xls', // <= Here: made sure file name match
            path: path.join(__dirname, '../public/files/Reporte_Clientes.xlsx'), // <= Here
            contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
    ]
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO REPORTE CLIENTES OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO REPORTE CLIENTE ERROR "+err);
        return false;
    });
    return estado;
}
/**********************************************/
/**********************************************/
/**********************************************/
const view_mail_reporte_gatillos = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_reporte_gatillos.pug', opciones);
    return juice(html);
}
exports.mail_reporte_gatillos = async(opciones) => {
    const html = view_mail_reporte_gatillos(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.email,
        subject: opciones.asunto,
        text,
        html,
        attachments: [
        {
            filename: 'reporte_gatillos_new.xls', // <= Here: made sure file name match
            path: path.join(__dirname, '../public/files/exceldespachos/reporte_gatillos.xls'), // <= Here
            contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
    ]
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NOTAS DE COBRO OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTAS DE COBRO ERROR "+err);
        return false;
    });
    return estado;
}
/**********************************************/
/**********************************************/
/**********************************************/
const view_mail_reporte_notasdecobro = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_reporte_notasdecobro.pug', opciones);
    return juice(html);
}
exports.mail_reporte_notasdecobro = async(opciones) => {
    const html = view_mail_reporte_notasdecobro(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.email,
        subject: opciones.asunto,
        text,
        html,
        attachments: [
        {
            filename: 'Reporte Notas Cobros.xlsx', // <= Here: made sure file name match
            path: path.join(__dirname, '../public/files/Reporte_Notas_De_Cobros.xlsx'), // <= Here
            contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
    ]
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NOTAS DE COBRO OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTAS DE COBRO ERROR "+err);
        return false;
    });
    return estado;
}
/**********************************************/
/**********************************************/
/**********************************************/
const view_mail_reporte_masterbd = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_reporte_masterbd.pug', opciones);
    return juice(html);
}
exports.mail_reporte_masterbd = async(opciones) => {
    const html = view_mail_reporte_masterbd(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.email,
        subject: opciones.asunto,
        text,
        html,
        attachments: [
        {
            filename: 'Reporte_Master_BD.xlsx', // <= Here: made sure file name match
            path: path.join(__dirname, '../public/files/Reporte_Master_BD.xlsx'), // <= Here
            contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
    ]
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO MASTER BD OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO MASTER BD ERROR "+err);
        return false;
    });
    return estado;
}
/**********************************************/
/**********************************************/
/**********************************************/

const view_mail_nuevo_usuario = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_nuevo_usuario.pug', opciones);
    return juice(html);
}

exports.mail_nuevo_usuario = async(opciones) => {
    const html = view_mail_nuevo_usuario(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        /*to: opciones.email,*/
        to: 'eduardo.vidal@wscargo.cl',
        subject: opciones.asunto,
        bcc:casillabcc,
        text,
        html
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NUEVO USUARIO OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NUEVO USUARIO ERROR "+err);
        return false;
    });
    return estado;
}

/**********************************************/
/**********************************************/
/**********************************************/

const view_mail_notificacion_tarifa = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_tarifa.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_tarifa = async(opciones) => {
    const html = view_mail_notificacion_tarifa(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        /*to: opciones.email,*/
        to: 'eduardo.vidal@wscargo.cl',
        subject: opciones.asunto,
        bcc:casillabcc,
        text,
        html
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NUEVA TARIFA OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NUEVA TARIFA ERROR "+err);
        return false;
    });
    return estado;
}

/**********************************************/
/**********************************************/
/**********************************************/

const view_mail_notificacion_planificacion_confirmada = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_planificacion_confirmada.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_planificacion_confirmada = async(opciones) => {
    const html = view_mail_notificacion_planificacion_confirmada(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.email,
        //cc: 'pbarria.reyes@gmail.com',
        subject: opciones.asunto,
        bcc:casillabcc,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO CONFIRMACION PLANFICACION OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO CONFIRMACION PLANFICACION ERROR "+err);
        return false;
    });
    return estado;
}

const view_notificacion_etiqueta_cliente = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_etiqueta_cliente.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_etiqueta_cliente = async(opciones) => {
    const html = view_notificacion_etiqueta_cliente(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.email,
        //cc: 'pbarria.reyes@gmail.com',
        subject: opciones.asunto,
        text,
        html,
        bcc:casillabcc,
        attachments: [
        {
            filename: 'etiqueta-wsc-'+opciones.fk_cliente+'.pdf', // <= Here: made sure file name match
            path: path.join('C:/Users/Administrator/Documents/wscargo/restserver/server/controllers/etiquetas/'+opciones.fk_cliente+'.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO ETIQUETA CLIENTE OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO ETIQUETA CLIENTE ERROR "+err);
        return false;
    });
    return estado;
}

/*
*   
*
*/
const view_mail_notificacion_proceso_documental = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_proceso_documental.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_proceso_documental = async(opciones) => {
    const html = view_mail_notificacion_proceso_documental(opciones);
    const text = htmltoText.fromString(html);
    
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.to,
        cc: opciones.cc,
        bcc:casillabcc,
        subject: opciones.asunto,
        text,
        html,
        attachments: opciones.attachment 
        
        /*[
            {
                filename: opciones.attachment[0].filename, // <= Here: made sure file name match
                path: opciones.attachment[0].path, // <= Here
                contentType: opciones.attachment[0].contentType
            },
            {
                filename: opciones.attachment[1].filename, // <= Here: made sure file name match
                path: opciones.attachment[1].path, // <= Here
                contentType: opciones.attachment[1].contentType
            }
        ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NOTIFICACION PROCESO DOCUMENTAL OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION PROCESO DOCUMENTAL ERROR "+err);
        return false;
    });
    return estado;
}


const view_mail_notificacion_recepcion = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_recepcion_new.pug', opciones);
    return juice(html);
}


exports.mail_notificacion_recepcion = async(opciones) => {

    console.log("CONFIG CORREO "+JSON.stringify(emailConfig));
    console.log("CONFIG CORREO "+JSON.stringify(emailConfig));
    console.log("CONFIG CORREO "+JSON.stringify(emailConfig));
    const html = view_mail_notificacion_recepcion(opciones);
    const text = htmltoText.fromString(html);
    let email1='wscargo@wscargo.cl';
   /* /*
    if(opciones.emailcomercial && opciones.emailcomercial.length>0){
        email1=opciones.emailcomercial;
    }*/
    let opcionesEmailWsc = {
        from: email1,
        to:'wscargo@wscargo.cl',
        bcc: opciones.email+';'+casillabcc,
        cliente: opciones.cliente,
        replyTo: opciones.emailcomercial,
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO CONFIRMACION RECEPCION OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO CONFIRMACION RECEPCION ERROR "+err);
        return false;
    });
    
    return estado;
}

const view_mail_notificacion_pago = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_pago.pug', opciones);
    return juice(html);
}


exports.mail_notificacion_pago = async(opciones) => {

    console.log("DATOS ADICIONALES "+JSON.stringify(opciones.datosAdicionales));
    const html = view_mail_notificacion_pago(opciones);
    const text = htmltoText.fromString(html);
    let email1='wscargo@wscargo.cl';

    let opcionesEmailWsc = {
        from: email1,
        to:'wscargo@wscargo.cl',
        bcc: opciones.email+';pagos@wscargo.cl',
        cliente: opciones.cliente,
        replyTo: opciones.emailcomercial,
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO CONFIRMACION RECEPCION OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO CONFIRMACION RECEPCION ERROR "+err);
        return false;
    });
    
    return estado;
}

const view_mail_notificacion_consolidacion_rapida = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_consolidacion_rapida.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_consolidacion_rapida = async(opciones) => {

    const html = view_mail_notificacion_consolidacion_rapida(opciones);
    const text = htmltoText.fromString(html);
    let email1='wscargo@wscargo.cl';
   /* /*
    if(opciones.emailcomercial && opciones.emailcomercial.length>0){
        email1=opciones.emailcomercial;
    }*/
    let opcionesEmailWsc = {
        from: email1,
        to:'wscargo@wscargo.cl',
        bcc: opciones.email+';'+casillabcc,
        cliente: opciones.cliente,
        replyTo: opciones.emailcomercial,
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO CONFIRMACION RECEPCION OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO CONFIRMACION RECEPCION ERROR "+err);
        return false;
    });
    
    return estado;
}

const view_mail_notificacion_confirmacion_consolidacion_rapida = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_confirmacion_consolidacion_rapida.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_confirmacion_consolidacion_rapida = async(opciones) => {

    const html = view_mail_notificacion_confirmacion_consolidacion_rapida(opciones);
    const text = htmltoText.fromString(html);
    let email1='wscargo@wscargo.cl';
   /* /*
    if(opciones.emailcomercial && opciones.emailcomercial.length>0){
        email1=opciones.emailcomercial;
    }*/
    let opcionesEmailWsc = {
        from: email1,
        to:'wscargo@wscargo.cl',
        bcc: opciones.email+';'+casillabcc,
        cliente: opciones.cliente,
        replyTo: opciones.emailcomercial,
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        attachments: [
            {
                filename: opciones.datosAdicionales.filename,
                path: opciones.datosAdicionales.path
            }
        ],
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO CONFIRMACION CONSOLIDACION RAPIDA OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO CONFIRMACION CONSOLIDACION RAPIDA ERROR "+err);
        return false;
    });
    
    return estado;
}

const view_mail_notificacion_contenedor_proforma = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_contenedor_proforma.pug', opciones);
    return juice(html);
}


const reemplazar_metadatos =async(texto,datos)=>{
	let str=lodash.cloneDeep(texto);
	if(datos){
        if(datos.razonSocial!=null){
            str=str.replace('[CLIENTE_NOMBRE]',datos.razonSocial);
        }
		if(datos.fk_consolidado!=null){
			str=str.replace('[NUMERO_SERVICIO]',datos.fk_consolidado);
		}
		if(datos.referencia!=null){
			str=str.replace('[REFERENCIA_SERVICIO]',datos.referencia);
		}else{
            str=str.replace('[REFERENCIA_SERVICIO]','');
        }
		if(datos.num_proveedores!=null){
			str=str.replace('[NUMERO_PROVEEDORES]',datos.num_proveedores);
		}
		if(datos.bultos!=null){
			str=str.replace('[NUMERO_BULTOS]',datos.bultos);
		}
		if(datos.peso!=null){
			str=str.replace('[KILOS]',datos.peso);
		}
		if(datos.volumen!=null){
			str=str.replace('[VOLUMEN]',datos.volumen);
		}
	}	
	return str;
}

const reemplazar_metadatos_asunto =async(texto,datos)=>{
	let str=lodash.cloneDeep(texto);
	if(datos){
		if(datos.fk_consolidado!=null){
			str=str.replace('[NUMERO_SERVICIO]',datos.fk_consolidado);
		}
		if(datos.referencia!=null && datos.referencia.length>0){
			str=str.replace('[REFERENCIA_SERVICIO]',datos.referencia);
		}else{
            str=str.replace('| Ref. [REFERENCIA_SERVICIO] |','|');
        }
		
	}	

    str=str.replace('[PUERTO_ORIGEN]','china');
	return str;
}

exports.mail_notificacion_contenedor_proforma = async(opciones) => {
    console.log('aca');

    const sendMail=async(opcionesEmail)=>{
        var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log("info",info)
        console.log(" ENVIO CORREO NOTIFICACION PROFORMA OK ");
            return true;
        }).catch(function(err){
            console.log(" ENVIO CORREO NOTIFICACION PROFORMA ERROR "+err);
            return false;
        });
    };
    
    if(opciones.servicios && opciones.servicios.length>0){
        for(let i=0;i<opciones.servicios.length;i++){
            let newText=lodash.cloneDeep(opciones.texto);
            newText=await reemplazar_metadatos(newText,opciones.servicios[i]);
            opciones.newTexto=newText;
            let comercial=await funcionesCompartidasCtrl.get_comercial_vigente_data(opciones.servicios[i].fk_cliente);
            if(comercial.rows && comercial.rows.length>0){
            opciones.comercial=comercial.rows[0];
            }

            let asunto=await reemplazar_metadatos_asunto(opciones.asunto,opciones.servicios[i]);
            let timeLineData=await get_data_time_linea(opciones.servicios[i].fk_servicio);
            opciones.timeline=timeLineData;
            const html = view_mail_notificacion_contenedor_proforma(opciones);
            const text = htmltoText.fromString(html);
            let filterEmails=[];
            if(opciones.emails && opciones.emails.length>0){
                filterEmails=opciones.emails.filter(x=>x.fk_cliente==opciones.servicios[i].fk_cliente);
                if(filterEmails && filterEmails.length>0){
                    for(let j=0;j< filterEmails.length;j++){

                        let opcionesEmail = {
                            from: 'wscargo@wscargo.cl',
                            to: 'wscargo@wscargo.cl',
                            replyTo:filterEmails[j].emailComercial,
                            bcc:filterEmails[j].emailCliente+';'+filterEmails[j].emailComercial+';'+casillabcc,
                            fecha:opciones.fecha,
                            subject: asunto/*+' (cliente '+filterEmails[j].fk_cliente+')'*/,
                            text,
                            html
                        };
            
                        await sendMail(opcionesEmail);
                    }
                }
            }

        }
        return true;
    }else{
        return false;
    }
/*
    if(opciones.emails && opciones.emails.length>0){

        for(let i=0;i< opciones.emails.length;i++){

            let opcionesEmail = {
                from: 'wscargo@wscargo.cl',
                to: 'wscargo@wscargo.cl',
                replyTo:opciones.emails[i].emailComercial,
                bcc:opciones.emails[i].emailCliente+';'+opciones.emails[i].emailComercial,
                fecha:opciones.fecha,
                subject: opciones.asunto+' (cliente '+opciones.emails[i].fk_cliente+')',
                text,
                html
            };

           await sendMail(opcionesEmail);
        }
        
        return true;
    }else{
        return false;
    }*/
}



const view_mail_notificacion_question = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_question.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_question = async (opciones) => {
    const html = view_mail_notificacion_question(opciones);
    const text = htmltoText.fromString(html);

    let remitente='wscargo@wscargo.cl';
    /*if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        remitente=opciones.comercial.email;
    }*/

    let opcionesEmail = {
      from: remitente,
      to: opciones.to,
      //cc: opciones.cc,
      bcc:casillabcc,
      subject: opciones.asunto,
      text,
      html,
      //attachments: opciones.attachment
    };
    var estado = await transport_TNM.sendMail(opcionesEmail).then(function (info) {
        //console.log("info",info);
        console.log(" ENVIO CORREO QUESTION OK ");
        return true;
      })
      .catch(function (err) {
        console.log(" ENVIO CORREO QUESTION ERROR " + err);
        return false;
      });
}

/**********************************************/
/**********************************************/

const view_mail_nota_de_cobro = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_nota_de_cobro.pug', opciones);
    return juice(html);
}

exports.mail_nota_de_cobro = async(opciones) => {
    const html = view_mail_nota_de_cobro(opciones);
    const text = htmltoText.fromString(html);

    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.destinatario,
        cc: opciones.copia_destinatario,
        bcc:casillabcc,
        replyTo: opciones.copia_destinatario,
        subject: opciones.asunto,
        text,
        html,
        attachments: [
            {
                filename: opciones.pdf_file_name,
                path: opciones.pdf_file
            },
        ],
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NOTA DE COBRO OK ");
        return true;
    }).catch(function(err){
        console.log("\n\n\n\n\n\n\n\nENVIO CORREO NOTA DE COBRO ERROR "+err);
        return false;
    });
    return estado;
}

/**********************************************/
/**********************************************/

const view_mail_nota_de_cobro_din = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_nota_de_cobro_din.pug', opciones);
    return juice(html);
}

exports.mail_nota_de_cobro_din = async(opciones) => {
    const html = view_mail_nota_de_cobro_din(opciones);
    const text = htmltoText.fromString(html);

    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.destinatario,
        cc: opciones.copia_destinatario,
        bcc:casillabcc,
        replyTo: opciones.copia_destinatario,
        subject: opciones.asunto,
        text,
        html,
        attachments: [
            {
                filename: opciones.pdf_file_name,
                path: opciones.pdf_file
            },
            {
                filename: opciones.din_pdf_file_name,
                path: opciones.din_pdf_file
            },
        ],
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NOTA DE COBRO (DIN) OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTA DE COBRO (DIN) ERROR "+err);
        return false;
    });
    return estado;
}

/**********************************************/
/**********************************************/

const view_mail_nota_de_cobro_factura = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_nota_de_cobro_factura.pug', opciones);
    return juice(html);
}

exports.mail_nota_de_cobro_factura = async(opciones) => {
    const html = view_mail_nota_de_cobro_factura(opciones);
    const text = htmltoText.fromString(html);

    console.log('\n\n ARCHIVOS QUE TENGO QUE ENVIAR '+JSON.stringify(opciones.archivos));
    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.email,
        cc: opciones.copia,
        bcc:casillabcc,
        replyTo: opciones.copia,
        subject: opciones.asunto,
        text,
        html,
        attachments: opciones.archivos,
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO NOTA DE COBRO (FACTURA) OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTA DE COBRO (FACTURA) ERROR "+err);
        return false;
    });
    return estado;
}

/**********************************************/
/**********************************************/

const view_mail_etiquetas_2022_clientes = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_etiquetas_2022.pug', opciones);
    return juice(html);
}

exports.mail_etiquetas_2022_clientes = async(opciones) => {
    const html = view_mail_etiquetas_2022_clientes(opciones);
    const text = htmltoText.fromString(html);

    let opcionesEmail = {
        from: 'wscargo@wscargo.cl',
        to: opciones.to,
        cc: opciones.cc,
        bcc:casillabcc,
        replyTo: opciones.emailEjecutivo,
        subject: opciones.subject,
        text,
        html,
        attachments: opciones.attachments
    };

    var estado = await transport_TNM.sendMail(opcionesEmail).then(function(info){
        console.log(" ENVIO CORREO ETIQUETAS 2022 OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO  ETIQUETAS 2022 ERROR "+err);
        return false;
    });
    return estado;
}

const view_mail_notificacion_exp_digital = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_exp_digital.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_exp_digital = async(opciones) => {
    const html = view_mail_notificacion_exp_digital(opciones);
    const text = htmltoText.fromString(html);
    let opcionesEmailWsc = {
        from: 'wscargo@wscargo.cl',
        to:'wscargo@wscargo.cl',
        bcc: opciones.email+';'+casillabcc,
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL ERROR "+err);
        return false;
    });
    
    return estado;
}


const view_mail_notificacion_1 = (opciones) => {
    const html = pug.renderFile('./views/emails/notificaciones/notif_1.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_1 = async(opciones) => {
console.log('aqui');
   console.log('opciones',opciones);
    let remitente='wscargo@wscargo.cl';
   /* if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        remitente=opciones.comercial.email;
    }*/
    let opcionesEmailWsc = {
        from: remitente,
        to:opciones.email,
        bcc:'wscargo@wscargo.cl;'+casillabcc,
        fecha:opciones.fecha,
        subject: opciones.asunto
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        opcionesEmailWsc.replyTo=opciones.comercial.email;
    }

    if(opciones.attachments){console.log('ADJUNTANDO ARCHIVO');
    if(opciones.attachments.tipo=='COTIZACION')
    {
        opcionesEmailWsc.attachments=[
            {
                filename: 'cotizacion_'+opciones.attachments.id+'.pdf', // <= Here: made sure file name match
                path: path.join(process.env.RutaLocalQiao+'/public/files/cotizaciones/cotizacion_'+opciones.attachments.id+'.pdf'), // <= Here
                contentType: 'application/pdf'
            }
        ]
        if(opciones.attachments.etiqueta && opciones.attachments.fk_cliente){console.log('ADJUNTANDO ETIQUETA');
            opcionesEmailWsc.attachments.push(
                {
                    filename: 'etiqueta_'+opciones.attachments.fk_cliente+'.pdf', // <= Here: made sure file name match
                    path: path.join(process.env.RutaLocalQiao+'/server/controllers/etiquetas/'+opciones.attachments.fk_cliente+'.pdf'), // <= Here
                    contentType: 'application/pdf'
                }
            );
        }
    }else if(opciones.attachments.tipo==11 && opciones.attachments.carpeta!=null && opciones.attachments.carpeta.length>0){//nota cobro
            let carpt=opciones.attachments.carpeta.slice(0,-2);
            opcionesEmailWsc.attachments=[
                {
                    filename: 'nota_cobro_'+opciones.attachments.carpeta+'.pdf', // <= Here: made sure file name match
                    path: path.join(process.env.RutaLocalQiao+'/public/files/notas_de_cobro/'+carpt+'/'+opciones.attachments.carpeta+'.pdf'), // <= Here
                    contentType: 'application/pdf'
                }
            ]
        }else if(opciones.attachments.tipo==12 && opciones.attachments.carpeta!=null && opciones.attachments.carpeta.length>0){//DIN
            let carpt=opciones.attachments.carpeta.slice(0,-2);

            var Correo = await client.query(` 
            SELECT din
            FROM public.notas_cobros
            WHERE fk_despacho = (SELECT id FROM public.despachos WHERE n_carpeta='${opciones.attachments.carpeta}')
            LIMIT 1
            `);

            if(Correo.rows[0].length > 0) {
                opcionesEmailWsc.attachments=[
                    {
                        filename: 'fact_iva_'+opciones.attachments.carpeta+'.pdf', // <= Here: made sure file name match
                        path: path.join(process.env.RutaLocalQiao+'/public/files/din/DIN'+Correo.rows[0]['din']+'.pdf'), // <= Here
                        contentType: 'application/pdf'
                    }
                ]
            }

        }
        else if(opciones.attachments.tipo==13 && opciones.attachments.carpeta!=null && opciones.attachments.carpeta.length>0)
        {//FACTURA
            let carpt=opciones.attachments.carpeta.slice(0,-2);

            opcionesEmailWsc.attachments.push(
                {
                    filename: 'factura_'+opciones.attachments.carpeta+'.pdf', // <= Here: made sure file name match
                    path: path.join(process.env.RutaLocalQiao+'/public/files/fact_cargar_por_contenedor/'+opciones.attachments.carpeta+'.pdf'), // <= Here
                    contentType: 'application/pdf'
                }
            );

            if (fs.existsSync(process.env.RutaLocalQiao+'/public/files/fact_cargar_por_contenedor/'+opciones.attachments.carpeta+'_EXENTA.pdf')) 
            {
                opcionesEmailWsc.attachments.push(
                    {
                        filename: 'factura_'+opciones.attachments.carpeta+'_exenta.pdf', // <= Here: made sure file name match
                        path: path.join(process.env.RutaLocalQiao+'/public/files/fact_cargar_por_contenedor/'+opciones.attachments.carpeta+'_EXENTA.pdf'), // <= Here
                        contentType: 'application/pdf'
                    }
                );
            }

            if (fs.existsSync(process.env.RutaLocalQiao+'/public/files/fact_cargar_por_contenedor/'+opciones.attachments.carpeta+'_AFECTA.pdf')) 
            {
                opcionesEmailWsc.attachments.push(
                    {
                        filename: 'factura_'+opciones.attachments.carpeta+'_afecta.pdf', // <= Here: made sure file name match
                        path: path.join(process.env.RutaLocalQiao+'/public/files/fact_cargar_por_contenedor/'+opciones.attachments.carpeta+'_AFECTA.pdf'), // <= Here
                        contentType: 'application/pdf'
                    }
                );
            }
        }
    }

    if(opciones.timeline && opciones.timeline!=null){
        let timeLineData=await get_data_time_linea(opciones.timeline);
        opciones.timeline=timeLineData;
     }
     const html = view_mail_notificacion_1(opciones);
     const text = htmltoText.fromString(html);
     opcionesEmailWsc.text=text;
     opcionesEmailWsc.html=html;
     

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL OK ");
        
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL ERROR "+err);
        return false;
    });
    console.log('estado correo handler',estado);
    return estado;
}

const view_mail_notificacion_6 = (opciones) => {
    const html = pug.renderFile('./views/emails/notificaciones/notif_6.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_6 = async(opciones) => {
    const html = view_mail_notificacion_6(opciones);
    const text = htmltoText.fromString(html);
    let remitente='wscargo@wscargo.cl';
    /*if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        remitente=opciones.comercial.email;
    }*/
    let opcionesEmailWsc = {
        from: remitente,
        to:opciones.email,
        bcc:'wscargo@wscargo.cl;'+casillabcc,
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    if(opciones && opciones.comercial && opciones.comercial!=null && opciones.comercial.email && opciones.comercial.email!=null){
        opcionesEmailWsc.replyTo=opciones.comercial.email;
    }

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION EXP DIGITAL ERROR "+err);
        return false;
    });
    
    return estado;
}

/*
*   NOTIFICACION DE ENCUESTA FINAL
*
*/

const view_mail_notificacion_32 = (opciones) => {
    const html = pug.renderFile('./views/emails/notificaciones/notif_32.pug', opciones.notificacion);
    return juice(html);
}

exports.mail_notificacion_32 = async(opciones) => {
    const html = view_mail_notificacion_32(opciones);
    const text = htmltoText.fromString(html);
    let opcionesEmailWsc = {
        from: opciones.from,
        to: opciones.to,
        bcc:casillabcc,
        subject: opciones.subject,
        text,
        html,
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NOTIFICACION 32 OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION 32 ERROR "+err);
        return false;
    });
    
    return estado;
}

/*
*   NOTIFICACION RETIRO PROGRAMADO
*
*/

const view_mail_notificacion_retiro_programado = (opciones) => {
    const html = pug.renderFile('./views/emails/notificaciones/notificacion_retiro_programado.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_retiro_programado = async(opciones) => {
    let payload = {
        nombreComercial: `${opciones.comercial.nombre} ${opciones.comercial.apellidos}`,
        fecha: moment(opciones.datos.dateReservationInicio).format('DD/MM/YYYY'),
        fk_cliente:opciones.datos.fk_cliente
    }

    const html = view_mail_notificacion_retiro_programado(payload);
    const text = htmltoText.fromString(html);
    let opcionesEmailWsc = {
        from: opciones.email,
        to:'wscargo@wscargo.cl',
        bcc:casillabcc,
        subject: opciones.asunto,
        text,
        html,
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NOTIFICACION 32 OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION 32 ERROR "+err);
        return false;
    });
    
    return estado;
}


const view_mail_notificacion_new_consolidado = (opciones) => {
    const html = pug.renderFile('./views/emails/view_mail_notificacion_new_consolidado.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_new_consolidado = async(opciones) => {

    const html = view_mail_notificacion_new_consolidado(opciones);
    const text = htmltoText.fromString(html);
    let email1='wscargo@wscargo.cl';
   /* /*
    if(opciones.emailcomercial && opciones.emailcomercial.length>0){
        email1=opciones.emailcomercial;
    }*/
    let opcionesEmailWsc = {
        from: email1,
        to:'wscargo@wscargo.cl',
        bcc: opciones.email+';'+casillabcc,
        cliente: opciones.cliente,
        replyTo: opciones.emailcomercial,
        fecha:opciones.fecha,
        subject: opciones.asunto,
        text,
        html,
        attachments:[
            {
                filename: 'cotizacion_'+opciones.attachments.id+'.pdf', // <= Here: made sure file name match
                path: path.join(process.env.RutaLocalQiao+'/public/files/cotizaciones/cotizacion_'+opciones.attachments.id+'.pdf'), // <= Here
                contentType: 'application/pdf'
            }
        ]
        /*attachments: [
        {
            filename: 'file-name.pdf', // <= Here: made sure file name match
            path: path.join(__dirname, '../server/controllers/etiquetas/Propuesta_Comercial_ejemplo.pdf'), // <= Here
            contentType: 'application/pdf'
        }
    ]*/
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NUEVO CONSOLIDADO SELECCIONABLE OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NUEVO CONSOLIDADO SELECCIONABLE ERROR "+err);
        return false;
    });
    
    return estado;
}

const get_data_time_linea =async(fk_servicio)=>{
    let sql=`SELECT DISTINCT sh.texto, MAX(sh.fecha) AS fecha, lt.position
    FROM public.servicio_historial sh
    INNER JOIN public.linea_tiempo lt ON TRIM(UPPER(sh.texto)) = TRIM(UPPER(lt.history))
    WHERE sh.fk_servicio = `+fk_servicio+` AND sh.estado = true AND lt.activo = true
    GROUP BY sh.texto, lt.position
    ORDER BY lt.position ASC`;
    let result=await client.query(sql);
    if (!result || !result.rows) {
        throw new Error('Problemas al intentar obtener el historial de Servicio');
    }

    let sqlTimeLine=`SELECT * FROM public.linea_tiempo WHERE activo=true ORDER BY position ASC`;
    let resultTimeLine=await client.query(sqlTimeLine);

    let sqlFechasEstimadas=`SELECT * FROM public."fechasEstimadas" order by id asc`;
    let resultFechasEstimadas=await client.query(sqlFechasEstimadas);

    let aux = [];
    let fechaEstimada = {};
    let estimacion = 0;
    let fecha = '';
    let yaLlegoAunoSin = false;


    let sql1=`SELECT c.id as fk_consolidado,
    (select t.fk_proforma from public.tracking t inner join public.consolidado_tracking ct on ct.fk_tracking=t.id where ct.fk_consolidado=c.id and ct.estado>=0 order by ct.id desc limit 1) as fk_proforma
    from public.consolidado c
    inner join public.gc_propuestas_cabeceras gc on gc.id=c.fk_propuesta
    where gc.fk_servicio=`+fk_servicio;
    let result1=await client.query(sql1);
    if(result1 && result1.rows && result1.rows.length>0){
        let sql2=`SELECT 
        fecha_retiro_puerto,
        fecha_prog_aforo,
        fecha_real_aforo,
        fecha_consolidado
        FROM public.contenedor_proforma where id=`+result1.rows[0].fk_proforma;
        let result2=await client.query(sql2);

        if(result2 && result2.rows && result2.rows.length>0){
            let find=result.rows.findIndex(x=>x.texto=='Carga consolidada');
            if(find>=0){
                if(result2.rows[0].fecha_consolidado!=null){
                    result.rows[find].fecha=result2.rows[0].fecha_consolidado;
                }
            }else{
                if(result2.rows[0].fecha_consolidado!=null){
                    result.rows.push({texto:'Carga consolidada',fecha:result2.rows[0].fecha_consolidado,fk_servicio:fk_servicio}); 
                }
            }

            let find2=result.rows.findIndex(x=>x.texto=='Programación aforo');
            if(find2>=0){
                if(result2.rows[0].fecha_prog_aforo!=null){
                    result.rows[find2].fecha=result2.rows[0].fecha_prog_aforo;
                }
            }else{
                if(result2.rows[0].fecha_prog_aforo!=null){
                    result.rows.push({texto:'Programación aforo',fecha:result2.rows[0].fecha_prog_aforo,fk_servicio:fk_servicio}); 
                }
            }

            let find3=result.rows.findIndex(x=>x.texto=='Real aforo');
            if(find3>=0){
                if(result2.rows[0].fecha_real_aforo!=null){
                    result.rows[find3].fecha=result2.rows[0].fecha_real_aforo;
                }
            }else{
                if(result2.rows[0].fecha_real_aforo!=null){
                    result.rows.push({texto:'Real aforo',fecha:result2.rows[0].fecha_real_aforo,fk_servicio:fk_servicio}); 
                }
            }

            let find4=result.rows.findIndex(x=>x.texto=='Retiro de puerto');
            if(find4>=0){
                if(result2.rows[0].fecha_retiro_puerto!=null){
                    result.rows[find4].fecha=result2.rows[0].fecha_retiro_puerto;
                    result.rows[find4].texto='Arribo puerto de destino';
                }
            }else{
                if(result2.rows[0].fecha_retiro_puerto!=null){
                    result.rows.push({texto:'Arribo puerto de destino',fecha:result2.rows[0].fecha_retiro_puerto,fk_servicio:fk_servicio,estado:true}); 
                }
            }
        }

        let sql3=`SELECT cp.id,
        cp.fk_contenedor,
        c.codigo as fk_contenedor_nombre,
        (SELECT n2.nave_nombre FROM naves2 n2 LEFT JOIN naves_eta ne on ne.fk_nave=n2.nave_id LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking  where ct.id=cp.fk_contenedor_tracking and ne.estado<2 order by ne.id asc limit 1) as nave_nombre,
        (SELECT n2.nave_id FROM naves2 n2 LEFT JOIN naves_eta ne on ne.fk_nave=n2.nave_id LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking  where ct.id=cp.fk_contenedor_tracking and ne.estado<2 order by ne.id asc limit 1) as nave_id
        FROM public.contenedor_proforma cp
        left join contenedor c on c.id=cp.fk_contenedor
        where cp.id=`+result1.rows[0].fk_proforma+`
        ORDER BY cp.id DESC`;
        let result3=await client.query(sql3);
        if(result3 && result3.rows && result3.rowCount>0){
            /*let sqlTnm=`select
                        cli_fact.cli_nombre as cli_fact_nombre
                        , cli_desp.cli_nombre as cli_desp_nombre
                        
                        , coalesce(nave.nave_imo,0) as servicio_nave_imo
                        , coalesce(nave.nave_id,0) as servicio_nave_id
                        , coalesce(nave.nave_nombre,'') as servicio_nave_nombre
                        , coalesce(eta.eta_fecha,'') as eta_fecha
                        , coalesce(eta.eta_hora,'') as eta_hora
                        
                        , ser.numero_contenedor
                        
                        , coalesce(eta_1.fecha, '') as etapa_1_fecha
                        , coalesce(eta_1.hora, '') as etapa_1_hora
                        , coalesce(dir_1.nombre,'') as etapa_1_lugar_nombre
                        
                        , coalesce(eta_2.fecha, '') as etapa_2_fecha
                        , coalesce(eta_2.hora, '') as etapa_2_hora
                        , coalesce(dir_2.nombre,'') as etapa_2_lugar_nombre
                        
                        from
                        public.servicios as ser
                        left join public.clientes as cli_fact on ser.fk_cliente_facturacion=cli_fact.cli_codigo
                        left join public.clientes as cli_desp on ser.fk_cliente_despacho=cli_desp.cli_codigo
                        left join public.naves as nave on ser.fk_nave=nave.nave_id
                        left join public.naves_etas as eta on ser.fk_eta=eta.eta_id
                        
                        left join public.servicios_etapas as eta_1 on ser.id=eta_1.fk_servicio and eta_1.tipo=1
                        left join public.direcciones as dir_1 on eta_1.fk_direccion=dir_1.id
                        left join public.comunas as com_1 on dir_1."comunaComunaId"=com_1.comuna_id
                        
                        left join public.servicios_etapas as eta_2 on ser.id=eta_2.fk_servicio and eta_2.tipo=2
                        left join public.direcciones as dir_2 on eta_2.fk_direccion=dir_2.id
                        
                        where
                        ser.estado!=999
                        and replace(ser.numero_contenedor, '-', '') = '`+result3.rows[0].fk_contenedor_nombre+`' and nave.nave_nombre='`+result3.rows[0].nave_nombre+`'`;

                        let rTnm=await clientTnm.query(sqlTnm);
                        let rTnm=false;
                
                        if(rTnm && rTnm.rows && rTnm.rowCount>0){
                            result.rows.push({texto:'Contenedor retirado de puerto',fecha:moment(rTnm.rows[0].etapa_1_fecha,'DD-MM-YYYY').format('YYYY-MM-DDT00:00:00'),fk_servicio:fk_servicio}); 
                        }*/

                        let sqlTnm=`SELECT *FROM public.tnm_fechas where contenedor='`+result3.rows[0].fk_contenedor_nombre+`' and nave='`+result3.rows[0].nave_id+`' and estado=true order by id desc`;
                        let rTnm=await client.query(sqlTnm);
                
                        if(rTnm && rTnm.rows && rTnm.rowCount>0){
                            if(rTnm.rows[0].fecha_retiro && rTnm.rows[0].fecha_retiro!=null){
                                result.rows.push({texto:'Contenedor retirado de puerto',fecha:moment(rTnm.rows[0].fecha_retiro,'DD-MM-YYYY').format('YYYY-MM-DDT00:00:00'),fk_servicio:fk_servicio});
                            }
                        }
                    }
    }

    for (let i = 0; i < resultTimeLine.rows.length; i++) {
        // Si no es el ultimo item
        if (i < resultTimeLine.rows.length - 1) {
            let item = resultTimeLine.rows[i];

            let existe = result.rows.findIndex(x => x.texto.toUpperCase().trim() === item.history.toUpperCase().trim());
            let existeHistory = result.rows.find(x => x.texto.toUpperCase().trim() === item.history.toUpperCase().trim());
            if (existe >= 0) {
                let existeFechasEstimadas = resultFechasEstimadas.rows.length > 0 ? resultFechasEstimadas.rows.find((f) => f.title === item.history) : 'undefined';

                if (existeFechasEstimadas !== 'undefined' && existeFechasEstimadas !== undefined) {
                fechaEstimada = existeFechasEstimadas;
                estimacion = parseInt(fechaEstimada.dias);
                fecha = moment(existeHistory.fecha).add(estimacion, 'days');
                }

                aux.push({
                id: item.id,
                texto: item.texto,
                history:item.history,
                estado: '1',
                fechaEstimada: moment(existeHistory.fecha).format('DD/MM/YY'),
                });

            } else {
                // Obtener la fecha estimada, si fecha y fechaEstimada estan definidas
                let fecha_proxima = '';
                if (fecha !== '' && fechaEstimada !== '' && !yaLlegoAunoSin) {
                    fecha_proxima = `Estimado entre ${moment(fecha).subtract(fechaEstimada.rango_inferior, 'days').format('DD/MM/YY')} - ${moment(fecha).add(fechaEstimada.rango_superior, 'days').format('DD/MM/YY')}`;
                }
                yaLlegoAunoSin = true;

                aux.push({
                    id: item.id,
                    texto: item.texto,
                    history:item.history,
                    estado: '-1',
                    fechaEstimada: fecha_proxima
                });
            }
        } else {
            let item = resultTimeLine.rows[i];

            let existe = result.rows.findIndex(x => x.texto.toUpperCase().trim() === item.history.toUpperCase().trim());
            
            if (existe >= 0) {
                let existeHistory = result.rows.find(x => x.texto.toUpperCase().trim() === item.history.toUpperCase().trim());
                
                aux.push({
                    id: item.id,
                    texto: item.texto,
                    history:item.history,
                    estado: '1',
                    fechaEstimada: moment(existeHistory.fecha).format('DD/MM/YY')
                })
            } else {
                let fecha_proxima = '';
                if (fecha !== '' && fechaEstimada !== '' && !yaLlegoAunoSin) {
                    fecha_proxima = `Estimado entre ${moment(fecha).subtract(fechaEstimada.rango_inferior, 'days').format('DD/MM/YY')} - ${moment(fecha).add(fechaEstimada.rango_superior, 'days').format('DD/MM/YY')} `;
                }
                
                aux.push({
                id: item.id,
                texto: item.texto,
                history:item.history,
                estado: '-1',
                fechaEstimada: fecha_proxima
                });
            }
        }
    }

    let ultimoCumplido = null;
    let proximoPorCumplir = null;
    for (let i = 0; i < aux.length; i++) {
        if (aux[i].estado=='1') {
        ultimoCumplido = aux[i];
        } else if (proximoPorCumplir === null && !aux[i].estado!='1') {
            proximoPorCumplir = aux[i];
        }
    }


    let findI=-1;
    let findI2=-1;

    findI=aux.findIndex(x=>x.id==ultimoCumplido.id);
    if(proximoPorCumplir!=null){
     findI2=aux.findIndex(x=>x.id==proximoPorCumplir.id);
    }

    
    for (let i = 0; i < aux.length; i++) {
        /**Marco todos los items antes del ultimo cumplido */
        if(i<=findI && aux[i].estado=='-1'){
            aux[i].estado='1';
           // aux[i].fecha='S/I';
            aux[i].fechaEstimada='S/I';
        }
       
    }

    if(findI>=0 && findI!=aux.length-1){
        let fechaUltimo=moment(ultimoCumplido.fechaEstimada,'DD/MM/YYYY');
        let fEstimada=resultFechasEstimadas.rows.find((f) => f.title === proximoPorCumplir.history);
        if(fEstimada!=='undefined' && fEstimada !== undefined && findI2>=0){
            aux[findI2].fechaEstimada=`Estimado entre ${moment(fechaUltimo).subtract(fEstimada.rango_inferior, 'days').format('DD/MM/YY')} - ${moment(fechaUltimo).add(fEstimada.rango_superior, 'days').format('DD/MM/YY')} `;
        }
       aux[aux.length-1].fechaEstimada=`Estimado entre ${moment(fecha).subtract(fechaEstimada.rango_inferior, 'days').format('DD/MM/YY')} - ${moment(fecha).add(fechaEstimada.rango_superior, 'days').format('DD/MM/YY')} `;;
    }

   
   // console.log('ultimoCumplido',ultimoCumplido);
    //console.log('proximoPorCumplir',proximoPorCumplir);
    //console.log('');
    console.log(aux);
    return aux;
}


const view_mail_notificacion_link_tracking = (opciones) => {
    const html = pug.renderFile('./views/emails/notificacion_link_tracking.pug', opciones);
    return juice(html);
}

exports.mail_notificacion_link_tracking = async(opciones) => {
    
    const html = view_mail_notificacion_link_tracking(opciones);
    const text = htmltoText.fromString(html);
    let opcionesEmailWsc = {
        from: opciones.email,
        to:'wscargo@wscargo.cl',
        bcc:casillabcc,
        subject: opciones.asunto,
        text,
        html,
    };

    var estado = await transport_TNM.sendMail(opcionesEmailWsc).then(function(info){
        console.log("info",info);
        console.log(" ENVIO CORREO NOTIFICACION LINK TRACKING OK ");
        return true;
    }).catch(function(err){
        console.log(" ENVIO CORREO NOTIFICACION LINK TRACKING ERROR "+err);
        return false;
    });
    
    return estado;
}