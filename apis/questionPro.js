/**
 * @desc Integracion de cargows con QuestionPro
 * @param 
 * @return 
 * @author ARIEL AGUILAR
 * @createAt 14-02-2022
 */
const axios = require('axios') ;
require('../server/config/config');

//DEFINIR EL ID DE LA ENCUESTA A UTILIZAR
const SURVEYID = process.env.SURVEYID;

/**
 * @desc Funcion para obtener todas las listas Addresses creadas 
 * @param object 
  {
    "name": "TEST EMAIL LIST ",
    "groupType": "Global"
  }
 * @return id del EmailList creado
 */
const getEmailList = async () => {
    console.log("OBTENGO EMAIL LIST CREADOS");
    try{
        const data = await axios.get(`${process.env.HOST_QUESTIONPRO}surveys/${SURVEYID}/emaillists?apiKey=${process.env.APIKEY_QUESTIONPRO}` );
        const { response } = data.data;
        return response;
    }catch(e){
        console.log("ha ocurrido un error en createEmailList "+e.messge);
    }
}

/**
 * @desc Funcion para crear una nueva lista 
 * @param object 
  {
    "name": "TEST EMAIL LIST ",
    "groupType": "Global"
  }
 * @return id del EmailList creado
 */
const createEmailList = async (emailList) => {
    console.log("SE CREA EL EMAIL LIST");
    try{
        const data = await axios.post(`${process.env.HOST_QUESTIONPRO}surveys/${SURVEYID}/emaillists?apiKey=${process.env.APIKEY_QUESTIONPRO}`, emailList );
        const { response } = data.data;
        return response.emailListID;
    }catch(e){
        console.log("ha ocurrido un error en createEmailList "+e.messge);
    }
}

/**
 * @desc Funcion para asignar a la nueva lista una lista de direcciones de contacto (emails)
 * @param array de object  
 * {
        "emailAddress": "jane.doe@emaildomain.com",
        "firstname": "Jane",
        "lastname": "Doe",
        "custom1": "WA",
        "custom2": "Sales",
        "custom3": "EID987",
        "custom4": "",
        "custom5": "",
        "highCustomVariables": {
            "custom8": "Product A",
            "custom10": "Department X"
		 }
    }
 * @return array de Object Email List
 */
const createEmailAddresses = async (emailListID, emailListAddress) => {
    console.log("SE CREA EMAIL ADDRESSES");
    try{
        const data = await axios.post(`${process.env.HOST_QUESTIONPRO}surveys/${SURVEYID}/emaillists/${emailListID}/emails?apiKey=${process.env.APIKEY_QUESTIONPRO}`, emailListAddress);
        const { response } = data.data;
        return response;
    }catch(e){
        console.log("ha ocurrido un error en createEmailAddresses "+e.messge);
    }
}

/**
 * @desc Funcion para asignar a la nueva lista una lista de direcciones de contacto (emails)
 * @param object  
 * {
      "emailListID" : 6375679
    }
 * @return array de Object Email List
 */
const createExportBatch = async (emailListID) => {
    console.log("SE CREA EL EXPORT BATCH");
    try{
        const data = await axios.post(`${process.env.HOST_QUESTIONPRO}surveys/${SURVEYID}/exportbatch?apiKey=${process.env.APIKEY_QUESTIONPRO}`, { emailListID: emailListID });
        const { response } = data.data;
        return response.emailBatchID;
    }catch(e){
        console.log("ha ocurrido un error en createExportBatch "+e.messge);
    }
}

/**
 * @desc Funcion para asignar a la nueva lista una lista de direcciones de contacto (emails)
 * @param emailListID  
 * @return array de Object Email List Addresses
 */

const getExportBatch = async (emailListID) => {
    console.log("OBTENGO EL EXPORT BATCH");
    try{
        const data = await axios.get(`${process.env.HOST_QUESTIONPRO}surveys/${SURVEYID}/emaillists/${emailListID}/exportbatch?page=1&perPage=100&apiKey=${process.env.APIKEY_QUESTIONPRO}`);
        const { response } = data.data;
        return response;

    }catch(e){
        console.log("ha ocurrido un error en getExportBatch "+e.messge);
    }
}

module.exports = { getEmailList, createEmailList, createEmailAddresses, createExportBatch, getExportBatch }