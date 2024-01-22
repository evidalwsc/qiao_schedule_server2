const client = require('../server/config/db.client');
const jwt=require('jsonwebtoken');
const lodash= require('lodash');
const moment=require('moment');

exports.recalcularPropuestas=async(arr)=>{
    try{
        if(arr && arr.length>0){
        	for(let i=0;i<arr.length;i++){
				let servicioQiao=await client.query(`
		            SELECT 
		            gc.id as fk_propuesta,
		            c.id as fk_consolidado,
		            gc.fk_cliente,
		            gc.direccion,
					gc.fk_servicio,
		            cl.rut,
		            cl.codigo,
		            cl."razonSocial",
		            cl.telefono1,
		            cl."dteEmail",
		            cl.giro,
		            cl.fk_comercial,
					cl."codigoSii",
					(select fk_region from public.clientes_direcciones where fk_cliente = cl.id and fk_tipo=1 order by id desc limit 1),
					(select fk_comuna from public.clientes_direcciones where fk_cliente = cl.id and fk_tipo=1 order by id desc limit 1),
					(select direccion from public.clientes_direcciones where fk_cliente = cl.id and fk_tipo=1 order by id desc limit 1),
					(select numero from public.clientes_direcciones where fk_cliente = cl.id and fk_tipo=1 order by id desc limit 1),
					(select id from public.clientes_direcciones where fk_cliente = cl.id and fk_tipo=1 order by id desc limit 1) as fk_direccion
		            FROM
		            public.gc_propuestas_cabeceras gc
		            INNER JOIN public.consolidado c on c.fk_propuesta=gc.id
		            INNER JOIN public.clientes cl on cl.id=gc.fk_cliente
		            WHERE gc.id=`+arr[i]+` and gc.estado>=0 and c.estado>=0
		        `);

		        if(servicioQiao && servicioQiao.rows && servicioQiao.rows.length>0){
					let tarifas=await client.query(`SELECT id,"fk_zonaOrigen","fk_zonaDestino","valorUnitarioUsd","tarifaUsd","cmbPeso","volumenEstimado","pesoEstimado","valorBaseUsd","unidadesACobrar" FROM public.gc_propuestas_tarifas WHERE fk_cabecera=`+servicioQiao.rows[0].fk_propuesta);
					let trackings=await client.query(`SELECT 
					t.id,
					t.cantidad_bultos,
					t.peso,
					t.volumen,
					t.fk_proveedor,
					p.nombre as fk_proveedor_nombre,
					p."nombreChi" as fk_proveedor_nombre_chino
					FROM public.tracking t 
					INNER JOIN public.consolidado_tracking ct on ct.fk_tracking=t.id
					INNER JOIN public.proveedores p on p.id=t.fk_proveedor
					where ct.fk_consolidado=`+servicioQiao.rows[0].fk_consolidado+` and t.estado>=0`);

					let bultos=0;let pesoEstimado=0;let volumenEstimado=0;

	                if(trackings && trackings.rows && trackings.rows.length>0){
	                    trackings.rows.map(item=>{
	                        bultos+=parseInt(item.cantidad_bultos);
	                        pesoEstimado+=parseFloat(item.peso);
	                        volumenEstimado+=parseFloat(item.volumen);
	                    })
	                }

	                
	                
	                let valorUnitarioUsd=0;
	                let tarifaUsd=0;
	                let valorBaseUsd=0;
	                let unidadesACobrar=0;
	                let fk_zonaOrigen=0;
	                let fk_zonaDestino=0;
	                let cmbPeso=0;
	                let nuevaTarifa=0;
	                let Pb_unidadesACobrar=0;
	                if(tarifas && tarifas.rows && tarifas.rows.length>0){
	                    for(let z=0;z<tarifas.rows.length;z++){
	                        if(z==0){
	                            valorUnitarioUsd=tarifas.rows[z].valorUnitarioUsd;
	                            valorBaseUsd=tarifas.rows[z].valorBaseUsd;
	                            //unidadesACobrar=tarifas.rows[z].unidadesACobrar;
	                            fk_zonaOrigen=tarifas.rows[z].fk_zonaOrigen;
	                            fk_zonaDestino=tarifas.rows[z].fk_zonaDestino;
	                            cmbPeso=tarifas.rows[z].cmbPeso;
	                           // Pb_unidadesACobrar=tarifas.rows[z].Pb_unidadesACobrar;
	                            let val=await reCalcularTarifa(volumenEstimado,pesoEstimado,valorUnitarioUsd,valorBaseUsd,cmbPeso,unidadesACobrar,Pb_unidadesACobrar);
	                       		tarifaUsd+=val[0];
	                       		unidadesACobrar=val[1];
	                       		console.log(`UPDATE public.gc_propuestas_tarifas SET "unidadesACobrar"=`+unidadesACobrar+`,"tarifaUsd"=`+tarifaUsd+` where id=`+tarifas.rows[z].id);
	                       		await client.query(`UPDATE public.gc_propuestas_tarifas SET "unidadesACobrar"=`+unidadesACobrar+`,"tarifaUsd"=`+tarifaUsd+`,"pesoEstimado"=`+pesoEstimado+`,"volumenEstimado"=`+volumenEstimado+` where id=`+tarifas.rows[z].id);
	                        }else{
	                        	tarifaUsd+=tarifas.rows[z].tarifaUsd;
	                    	}
	                    }
	                }
	                console.log(`UPDATE public.gc_propuestas_cabeceras SET "pesoEstimado"=`+pesoEstimado+` and "volumenEstimado"=`+volumenEstimado+` where id=`+servicioQiao.rows[0].fk_propuesta);
	              let sqlUpdateP=await client.query(`UPDATE public.gc_propuestas_cabeceras SET "pesoEstimado"=`+pesoEstimado+`,"volumenEstimado"=`+volumenEstimado+` where id=`+servicioQiao.rows[0].fk_propuesta);
				}
			}
        }
    }catch(error){
        console.log(error);
	    return null;
    }
}

const reCalcularTarifa=async(volumenEstimado,pesoEstimado,valorUnitarioUsd,valorBaseUsd,cmbPeso,unidadesACobrar,Pb_unidadesACobrar) =>{
        var Pb_unidadesACobrar = Pb_unidadesACobrar;
        Pb_unidadesACobrar = Pb_unidadesACobrar <= 0 ? unidadesACobrar : Pb_unidadesACobrar;

        if (!volumenEstimado) { volumenEstimado = 0; }
        if (!pesoEstimado) { pesoEstimado = 0; }
        if (!valorUnitarioUsd) { valorUnitarioUsd = 0; }
        if (!valorBaseUsd) { valorBaseUsd = 0; }
    
        var Peso = Math.round(parseFloat(pesoEstimado) / parseFloat(cmbPeso) * 100) / 100;
    
        if (Number(volumenEstimado) == Number(Peso)) { var Unidades = volumenEstimado; }
        else if (Number(volumenEstimado) > Number(Peso)) { var Unidades = volumenEstimado; }
        else if (Number(Peso) > Number(volumenEstimado)) { var Unidades = Peso; }
        else { var Unidades = 0; }
    
        if (Number(Unidades) < 1) { Unidades = 1; }
    
        var tarifaUsd = Number(Unidades) * parseFloat(valorUnitarioUsd);
    
        if (Number(Unidades) < Number(Pb_unidadesACobrar)) { tarifaUsd = Number(tarifaUsd) + Number(valorBaseUsd); }
        tarifaUsd = Math.round(tarifaUsd);
        return [tarifaUsd, Unidades];
 }