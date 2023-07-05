using Oracle.DataAccess.Client;
using OracleLibrary;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Web;
using Tipos;

namespace Agronegocios.Net.Models
{
    public class ServicioGranario
    {
        public List<Granario> GetFijaciones_Pesificaciones(string cuit)
        {
            try
            {
                DataSet ds = new DataSet();
                using (DBOracle db = new DBOracle())
                using (OracleConnection conection = db.NewConnection())
                {
                    OracleCommand command_Fija_Pesifica = conection.CreateCommand();
                    command_Fija_Pesifica.CommandType = System.Data.CommandType.Text;
                    command_Fija_Pesifica.BindByName = true;
                    command_Fija_Pesifica.CommandText = "  SELECT null IDslipAN,'AC' OrigenAdmis, continterno continterno , vendcuit CUITvend, vendnombre,VENDSTIPPRO,compcuit CUITcomp, compnombre,COMPSTIPPRO, ";
                    command_Fija_Pesifica.CommandText += " to_char(date_c(fecha), 'dd-mm-yyyy') fecha, a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco, a.zona zona, x.cuit destino, a.destino ctadestino, ";
                    command_Fija_Pesifica.CommandText += " y.nombre nomdestino, y.stipprovee, negocio negocio, B_Tb0('TIP_OPER', operacion, 'OP2')  operacion, grano producto, ";
                    command_Fija_Pesifica.CommandText += " substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha, contrato NroContrato, tipoprecio tipoprecio, precio precio, ";
                    command_Fija_Pesifica.CommandText += " decode(moneda, 'PE', '0', '1') moneda , ttmaxfin TTPactadas, 0 TTentregadas , ittaplicadas TTaplicadas, ";
                    command_Fija_Pesifica.CommandText += " ittretiro TTretiro, 0 TTfijadas , ittfijadas TTpesificadas, ittliqttotal TTliquidadas,0 TTpendAN ";
                    command_Fija_Pesifica.CommandText += " FROM acopio.contrato a, mcuit x, mcuenta y ";
                    command_Fija_Pesifica.CommandText += " WHERE y.empresa = a.empresa ";
                    command_Fija_Pesifica.CommandText += " AND y.cuenta = a.destino ";
                    command_Fija_Pesifica.CommandText += " AND x.ctamadre = y.ctamadre ";
                    command_Fija_Pesifica.CommandText += " AND status = '50' ";
                    command_Fija_Pesifica.CommandText += " AND to_char(date_c(fecha),'yyyy')> '2017' ";
                    command_Fija_Pesifica.CommandText += " AND tipoprecio  in ('HP') ";
                    command_Fija_Pesifica.CommandText += " AND ittaplicadas > ittfijadas ";
                    command_Fija_Pesifica.CommandText += " AND ittaplicadas > ittliqttotal ";
                    command_Fija_Pesifica.CommandText += " AND cosecha > '1718' ";
                    if (!string.IsNullOrEmpty(cuit))
                    {
                        command_Fija_Pesifica.CommandText += " and (vendcuit = :parametroCUIT)";
                        db.AddParameter(command_Fija_Pesifica, "parametroCUIT", cuit.ToString());
                    }
                    command_Fija_Pesifica.CommandText += " UNION ALL ";
                    command_Fija_Pesifica.CommandText += " SELECT null IDslipAN, 'AC' OrigenAdmis ,continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, compcuit CUITcomp, compnombre, COMPSTIPPRO, ";
                    command_Fija_Pesifica.CommandText += " to_char(date_c(fecha), 'dd-mm-yyyy') fecha,a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco , a.zona zona, x.cuit destino, a.destino ctadestino, ";
                    command_Fija_Pesifica.CommandText += " y.nombre nomdestino, y.stipprovee, negocio negocio, B_Tb0('TIP_OPER', operacion, 'OP2')  operacion, grano producto, ";
                    command_Fija_Pesifica.CommandText += " substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha , contrato NroContrato, tipoprecio tipoprecio, precio precio, ";
                    command_Fija_Pesifica.CommandText += " decode(moneda, 'PE', '0', '1') moneda, ttmaxfin TTPactadas, 0 TTentregadas, ittaplicadas TTaplicadas, ";
                    command_Fija_Pesifica.CommandText += " ittretiro TTretiro, 0 TTfijadas, ittfijadas TTpesificadas, ittliqttotal TTliquidadas,0 TTpendAN ";
                    command_Fija_Pesifica.CommandText += " FROM acopio.contrato a, mcuit x, mcuenta y ";
                    command_Fija_Pesifica.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                    command_Fija_Pesifica.CommandText += " AND x.ctamadre = y.ctamadre ";
                    command_Fija_Pesifica.CommandText += " AND status = '50' ";
                    command_Fija_Pesifica.CommandText += " AND to_char(date_c(fecha),'yyyy')> '2017' ";
                    command_Fija_Pesifica.CommandText += " AND tipoprecio  in ('AF') ";
                    command_Fija_Pesifica.CommandText += " AND ittaplicadas > ittfijadas ";
                    command_Fija_Pesifica.CommandText += " AND ittaplicadas > ittliqttotal ";
                    command_Fija_Pesifica.CommandText += " AND cosecha > '1718' ";
                    if (!string.IsNullOrEmpty(cuit))
                    {
                        command_Fija_Pesifica.CommandText += " and (vendcuit = :parametroCUIT)";
                        db.AddParameter(command_Fija_Pesifica, "parametroCUIT", cuit.ToString());
                    }
                    command_Fija_Pesifica.CommandText += " UNION ALL ";
                    command_Fija_Pesifica.CommandText += " SELECT null IDslipAN,'AC' OrigenAdmis, continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, compcuit CUITcomp, compnombre, compSTIPPRO, ";
                    command_Fija_Pesifica.CommandText += " to_char(date_c(fecha), 'dd-mm-yyyy') fecha,a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco, null zona , x.cuit destino, a.destino ctadestino, ";
                    command_Fija_Pesifica.CommandText += "  y.nombre nomdestino, y.stipprovee tipodestino, negocio negocio,B_Tb0('TIP_OPER', operacion, 'OP2') operacion, grano producto, ";
                    command_Fija_Pesifica.CommandText += "  substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha, contrato NroContrato, tipoprecio tipoprecio,precio precio, ";
                    command_Fija_Pesifica.CommandText += "         decode(substr(B_Tb0('MONEDA', a.moneda, 'MN61'), 1, 1), '1', '0', '1') moneda, ttmaxfin TTPactadas, 0 TTentregadas, ittaplicadas TTaplicadas, ";
                    command_Fija_Pesifica.CommandText += " 0 TTretiro, 0 TTfijadas, ittfijadas TTpesificadas, ittliqttotal TTliquidadas, 0 TTpendAN ";
                    command_Fija_Pesifica.CommandText += " FROM corretaje.contrato a, mcuit x, mcuenta y ";
                    command_Fija_Pesifica.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                    command_Fija_Pesifica.CommandText += " AND x.ctamadre = y.ctamadre ";
                    command_Fija_Pesifica.CommandText += " AND status = '50' ";
                    command_Fija_Pesifica.CommandText += " AND to_char(date_c(fecha),'yyyy')> '2017' ";
                    command_Fija_Pesifica.CommandText += " AND tipoprecio  in ('AF') ";
                    command_Fija_Pesifica.CommandText += " AND ittaplicadas > ittfijadas and ittaplicadas > ittliqttotal ";
                    command_Fija_Pesifica.CommandText += " AND cosecha > '1718' ";
                    if (!string.IsNullOrEmpty(cuit))
                    {
                        command_Fija_Pesifica.CommandText += " AND (vendcuit = :parametroCUIT1 or compCUIT = :parametroCUIT2)";
                        db.AddParameter(command_Fija_Pesifica, "parametroCUIT1", cuit.ToString());
                        db.AddParameter(command_Fija_Pesifica, "parametroCUIT2", cuit.ToString());
                    }
                    command_Fija_Pesifica.CommandText += " UNION ALL ";
                    command_Fija_Pesifica.CommandText += " SELECT null IDslipAN, 'AC' OrigenAdmis, continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, compcuit CUITcomp, compnombre, compSTIPPRO, ";
                    command_Fija_Pesifica.CommandText += " to_char(date_c(fecha), 'dd-mm-yyyy') fecha,a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco, null zona, x.cuit destino, a.destino ctadestino, ";
                    command_Fija_Pesifica.CommandText += " y.nombre nomdestino, y.stipprovee tipodestino, negocio negocio, B_Tb0('TIP_OPER', operacion, 'OP2') operacion, grano producto, ";
                    command_Fija_Pesifica.CommandText += " substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha, contrato NroContrato, tipoprecio tipoprecio, precio precio, ";
                    command_Fija_Pesifica.CommandText += " decode(substr(B_Tb0('MONEDA', a.moneda, 'MN61'), 1, 1), '1', '0', '1') moneda, ttmaxfin TTPactadas,0 TTentregadas, ittaplicadas TTaplicadas, ";
                    command_Fija_Pesifica.CommandText += " 0 TTretiro, 0 TTfijadas,ittfijadas TTpesificadas, ittliqttotal TTliquidadas, 0 TTpendAN ";
                    command_Fija_Pesifica.CommandText += " FROM corretaje.contrato a, mcuit x, mcuenta y ";
                    command_Fija_Pesifica.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                    command_Fija_Pesifica.CommandText += " AND x.ctamadre = y.ctamadre ";
                    command_Fija_Pesifica.CommandText += " AND status = '50'  and to_char(date_c(fecha),'yyyy')> '2017' ";
                    command_Fija_Pesifica.CommandText += " AND tipoprecio  in ('HP') ";
                    command_Fija_Pesifica.CommandText += " AND ittaplicadas > ittfijadas and ittaplicadas > ittliqttotal ";
                    command_Fija_Pesifica.CommandText += " AND cosecha > '1718' ";
                    if (!string.IsNullOrEmpty(cuit))
                    {
                        command_Fija_Pesifica.CommandText += " AND (vendcuit = :parametroCUIT1 or compCUIT = :parametroCUIT2)";
                        db.AddParameter(command_Fija_Pesifica, "parametroCUIT1", cuit.ToString());
                        db.AddParameter(command_Fija_Pesifica, "parametroCUIT2", cuit.ToString());
                    }
                    using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command_Fija_Pesifica))
                    {
                        dataAdapter.Fill(ds);
                    }
                    List<Granario> result = new List<Granario>();
                    result = ds.Tables[0].ToObject<Granario>().ToList();
                    return result;
                } 
            } catch (Exception exep)
                { throw exep; }
        }

        public List<Granario> GetReduccionesContratos(string cuit)
        {
            DataSet ds = new DataSet();
            using (DBOracle db = new DBOracle())
            using (OracleConnection conection = db.NewConnection())
            {
                OracleCommand command_Reducciones_Contratos = conection.CreateCommand();
                command_Reducciones_Contratos.CommandType = System.Data.CommandType.Text;
                command_Reducciones_Contratos.BindByName = true;
                command_Reducciones_Contratos.CommandText = " SELECT null IDslipAN, 'AC' OrigenAdmis, continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, compcuit CUITcomp, compnombre, COMPSTIPPRO, to_char(date_c(fecha), 'dd-mm-yyyy') fecha, ";
                command_Reducciones_Contratos.CommandText += " a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco, a.zona zona, x.cuit destino, a.destino ctadestino, y.nombre nomdestino, y.stipprovee, negocio negocio, ";
                command_Reducciones_Contratos.CommandText += " B_Tb0('TIP_OPER', operacion, 'OP2')  operacion, grano producto, substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha , contrato NroContrato, tipoprecio tipoprecio, ";
                command_Reducciones_Contratos.CommandText += " precio precio, decode(moneda, 'PE', '0', '1') moneda , ttmaxfin TTPactadas, 0 TTentregadas, ittaplicadas TTaplicadas, ittretiro TTretiro, 0 TTfijadas, ";
                command_Reducciones_Contratos.CommandText += " ittfijadas TTpesificadas, ittliqttotal TTliquidadas, 0 TTpendAN ";
                command_Reducciones_Contratos.CommandText += " FROM acopio.contrato a, mcuit x, mcuenta y ";
                command_Reducciones_Contratos.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                command_Reducciones_Contratos.CommandText += " AND x.ctamadre = y.ctamadre ";
                command_Reducciones_Contratos.CommandText += " AND status = '50' ";
                command_Reducciones_Contratos.CommandText += " AND cosecha > '1718' ";
                if (!string.IsNullOrEmpty(cuit))
                {
                    command_Reducciones_Contratos.CommandText +=  " and vendcuit = :parametroCUIT ";
                    db.AddParameter(command_Reducciones_Contratos, "parametroCUIT", cuit.ToString());
                }
                command_Reducciones_Contratos.CommandText += " UNION ALL ";
                command_Reducciones_Contratos.CommandText += " SELECT null IDslipAN , 'AC' OrigenAdmis, continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, compcuit CUITcomp, compnombre, compSTIPPRO, to_char(date_c(fecha), 'dd-mm-yyyy') fecha, ";
                command_Reducciones_Contratos.CommandText += " a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco , null zona , x.cuit destino, a.destino ctadestino, y.nombre nomdestino, y.stipprovee tipodestino, ";
                command_Reducciones_Contratos.CommandText += " negocio negocio, B_Tb0('TIP_OPER', operacion, 'OP2') operacion, grano producto, substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha, contrato NroContrato, ";
                command_Reducciones_Contratos.CommandText += " tipoprecio tipoprecio, precio precio, decode(substr(B_Tb0('MONEDA', a.moneda, 'MN61'), 1, 1), '1', '0', '1') moneda , ttmaxfin TTPactadas, 0 TTentregadas, ittaplicadas TTaplicadas, ";
                command_Reducciones_Contratos.CommandText += " 0 TTretiro, 0 TTfijadas, ittfijadas TTpesificadas, ittliqttotal TTliquidadas, 0 TTpendAN ";
                command_Reducciones_Contratos.CommandText += " FROM corretaje.contrato a, mcuit x, mcuenta y ";
                command_Reducciones_Contratos.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                command_Reducciones_Contratos.CommandText += " AND x.ctamadre = y.ctamadre ";
                command_Reducciones_Contratos.CommandText += " AND status = '50' ";
                command_Reducciones_Contratos.CommandText += " AND cosecha > '1718' ";
                if (!string.IsNullOrEmpty(cuit))
                {
                    command_Reducciones_Contratos.CommandText += " and(vendcuit = :parametroCUIT1 or compCUIT = :parametroCUIT2)";
                    db.AddParameter(command_Reducciones_Contratos, "parametroCUIT1", cuit.ToString());
                    db.AddParameter(command_Reducciones_Contratos, "parametroCUIT2", cuit.ToString());
                }
                using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command_Reducciones_Contratos))
                {
                    dataAdapter.Fill(ds);
                }
                List<Granario> result = new List<Granario>();
                result = ds.Tables[0].ToObject<Granario>().ToList();
                return result;
            }
        }
    }
}