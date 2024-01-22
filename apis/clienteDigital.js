/**
 * @desc Integracion de QIAO con CLIENTE DIGITAL
 * @param
 * @return
 * @author ARIEL AGUILAR
 * @createAt 07-07-2022
 * @updateAt 07-07-2022
 */
//require("../server/config/config");
const client = require('../server/config/db.client');
const axios = require("axios");
const qs = require("qs");
const QRCode = require("qrcode");
const PDF = require('pdfkit');//Importando la libreria de PDFkit
const path = require('path');
const fs = require('fs');

 
/**
 * 
 * funciones para solicitar informacion a Cliente Digital
 * @returns 
 */

const getAccessToken = async () => {
    //console.log("OBTENGO UN NUEVO TOKEN");
    const options = {
        method: "POST",
        url: `${process.env.systemClienteDigital}token`,
        data: {
            usuario: process.env.userLoginClienteDigital,
            password: process.env.passwordLoginClienteDigital
        },
    };
    try {
        const response = await axios(options);
        const { token }  = response.data;
        return token;
    } catch (e) {
        console.log("ha ocurrido un error en getAccessToken " + e.message);
    }
} 
const createClienteInClienteDigital = async (access_token, cliente) => {
    //console.log("OBTENGO UN NUEVO TOKEN");
    const options = {
        method: "POST",
        url: `${process.env.systemClienteDigital}import_cliente_qiao`,
        headers:{
            Authorization: access_token
        },
        data: {
            cliente: cliente,
        },
    };
    try {
        const response = await axios(options);
        return response.data;
    } catch (e) {
        console.log("ha ocurrido un error en getAccessToken " + e.message);
    }
}

/**
 * 
 * funciones para solicitar informacion a BBDD
 * @returns 
 */
const getCliente = async (id) => {
    try {
      let sql = 
      `select 
      cl.id, 
      cl.giro,
      cl.telefono1, 
      cl.telefono2,
      cl.rut, 
      cl."dteEmail", 
      cl.codigo, 
      cl."codigoSii",
      cl."razonSocial", 
      cl.fk_comercial,
      cl."codigoSii",
      (select fk_tipo from public.clientes_direcciones where fk_cliente = cl.id limit 1),
      (select fk_region from public.clientes_direcciones where fk_cliente = cl.id  limit 1),
      (select fk_comuna from public.clientes_direcciones where fk_cliente = cl.id  limit 1),
      (select direccion from public.clientes_direcciones where fk_cliente = cl.id  limit 1),
      (select numero from public.clientes_direcciones where fk_cliente = cl.id  limit 1),
      (select id from public.clientes_direcciones where fk_cliente = cl.id  limit 1) as fk_direccion
	  --cc.nombre,
	  --cc.apellido,
	  --cc.rut as "rutRepLegal",
	  --cc.email,
	  --cc.telefono_1
      from public.clientes cl
	  --inner join public.clientes_contactos cc on cl.id = cc.fk_cliente
      where cl.id = ${id} 
      --AND cc.fk_tipo = 4 limit 1 ; `;
  
      let result = await client.query(sql);
      
      return result.rows[0];

    } catch (e) {
      console.log("Error en getCliente :" + e.message);
    }
};
const getRepLegal = async (fk_cliente) => {
    try {
      let sql = `
      select 
      nombre as "NombreRepLegal",
        apellido as "ApellidoRepLegal",
        rut as "rutRepLegal",
        email as "emailRepLegal",
        telefono_1 as "telefonoRepLegal"
      from public.clientes_contactos 
      where fk_cliente = ${fk_cliente} 
      AND fk_tipo = 4 limit 1 ;`;
  
      let result = await client.query(sql);
  
      if(result && result.rows && result.rows.length > 0){
        return result.rows[0];
      }else{
        return { 
          NombreRepLegal:'',
          ApellidoRepLegal:'',
          rutRepLegal:'',
          emailRepLegal:'',
          telefonoRepLegal:'' 
        };
      }
    } catch (e) {
      console.log("Error en getRepLegal :" + e.message);
    }
}

/**
 * 
 * funciones para crear payloads para Zoho
 * @returns 
 */
 const createData = (
    { id, giro, telefono1, telefono2, rut, dteEmail, codigo, codigoSii, razonSocial, fk_comercial, fk_tipo, fk_region, fk_comuna, direccion, numero, fk_direccion},
    { NombreRepLegal,
        ApellidoRepLegal,
        rutRepLegal,
        emailRepLegal,
        telefonoRepLegal },
  ) => {
    try {
    
      return {
        id: id,
        rut: rut,
        dteEmail: dteEmail,
        razonSocial: razonSocial,
        telefono1: telefono1,
        giro: giro,
        fk_tipo: fk_tipo,
        fk_region: fk_region,
        fk_comuna: fk_comuna,
        direccion: direccion,
        numero: numero,
        fk_comercial: fk_comercial,
        fk_direccion: fk_direccion,
        codigoSii:codigoSii,
        nombre:NombreRepLegal,
        apellido:ApellidoRepLegal,
        telefono_1:telefonoRepLegal,
        rutRepLegal:rutRepLegal,
        email: emailRepLegal
      };
    } catch (e) {
      console.log("Ha ocurrido un error en createData : " + e.message);
    }
};

/**
 * 
 * @description funciones de procesos de carga o envio de informacion
 * 
 */
const SendCreateCliente = async (id) => {
    const cliente = await getCliente(id);
    const representante = await getRepLegal(id);
    const data = createData(cliente, representante)
    const access_token = await getAccessToken();
    const response = await createClienteInClienteDigital(access_token, data);
    //console.log("Cliente Digital Responde", response);
}


module.exports = { 
    SendCreateCliente
}