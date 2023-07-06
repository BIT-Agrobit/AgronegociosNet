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
        public List<Granario> GetFijaciones_Pesificaciones(string cuit, string nroCuenta)
        {
            try
            {
                DataSet ds = new DataSet();
                using (DBOracle db = new DBOracle())
                using (OracleConnection conection = db.NewConnection())
                {
                    OracleCommand command_Fija_Pesifica = conection.CreateCommand();
                    command_Fija_Pesifica.CommandType = System.Data.CommandType.Text;

                    string whereNroCuenta = "";
                    if (!string.IsNullOrEmpty(nroCuenta))
                        whereNroCuenta = string.Format(" and a.cuenta = {0} ", nroCuenta.ToString());

                    command_Fija_Pesifica.BindByName = true;
                    command_Fija_Pesifica.CommandText = string.Format("WITH tmpcuenta AS (SELECT cuenta FROM Mcuenta a INNER JOIN mcuit b ON (a.ctamadre = b.ctamadre AND b.cuit = :parametroCUIT {0}))", whereNroCuenta);
                    command_Fija_Pesifica.CommandText += "SELECT null IDslipAN,'AC' OrigenAdmis, continterno continterno , vendcuit CUITvend, vendnombre,VENDSTIPPRO, a.vendcta VendCta, compcuit CUITcomp, compnombre,COMPSTIPPRO, a.compcta CompCta, ";
                    command_Fija_Pesifica.CommandText += " to_char(date_c(fecha), 'dd-mm-yyyy') fecha, a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco, a.zona zona, x.cuit destino, a.destino ctadestino, ";
                    command_Fija_Pesifica.CommandText += " y.nombre nomdestino, y.stipprovee, negocio negocio, B_Tb0('TIP_OPER', operacion, 'OP2')  operacion, grano producto, ";
                    command_Fija_Pesifica.CommandText += " substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha, contrato NroContrato, tipoprecio tipoprecio, precio precio, ";
                    command_Fija_Pesifica.CommandText += " decode(moneda, 'PE', '0', '1') moneda , ttmaxfin TTPactadas, 0 TTentregadas , ittaplicadas TTaplicadas, ";
                    command_Fija_Pesifica.CommandText += " ittretiro TTretiro, 0 TTfijadas , ittfijadas TTpesificadas, ittliqttotal TTliquidadas,0 TTpendAN ";
                    command_Fija_Pesifica.CommandText += " FROM acopio.contrato a, mcuit x, mcuenta y, tmpcuenta cta ";
                    command_Fija_Pesifica.CommandText += " WHERE y.empresa = a.empresa ";
                    command_Fija_Pesifica.CommandText += " AND y.cuenta = a.destino ";
                    command_Fija_Pesifica.CommandText += " AND x.ctamadre = y.ctamadre ";
                    command_Fija_Pesifica.CommandText += " AND a.empresa = 1 AND a.status = '50' ";
                    //command_Fija_Pesifica.CommandText += " AND to_char(date_c(fecha),'yyyy')> '2017' ";
                    command_Fija_Pesifica.CommandText += " AND a.fecha > ndate_c('01/01/2018') ";
                    command_Fija_Pesifica.CommandText += " AND a.tipoprecio in ('HP', 'AF') ";
                    command_Fija_Pesifica.CommandText += " AND a.ittaplicadas > a.ittfijadas ";
                    command_Fija_Pesifica.CommandText += " AND a.ittaplicadas > a.ittliqttotal ";
                    command_Fija_Pesifica.CommandText += " and (a.vendcta = cta.cuenta)";
                    //db.AddParameter(command_Fija_Pesifica, "parametroCUIT", cuit.ToString());
                    //if (!string.IsNullOrEmpty(cuit))
                    //{
                    //    command_Fija_Pesifica.CommandText += " and (vendcuit = :parametroCUIT)";
                    //    db.AddParameter(command_Fija_Pesifica, "parametroCUIT", cuit.ToString());
                    //}
                    //command_Fija_Pesifica.CommandText += " UNION ALL ";
                    //command_Fija_Pesifica.CommandText += " SELECT null IDslipAN, 'AC' OrigenAdmis ,continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, compcuit CUITcomp, compnombre, COMPSTIPPRO, ";
                    //command_Fija_Pesifica.CommandText += " to_char(date_c(fecha), 'dd-mm-yyyy') fecha,a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco , a.zona zona, x.cuit destino, a.destino ctadestino, ";
                    //command_Fija_Pesifica.CommandText += " y.nombre nomdestino, y.stipprovee, negocio negocio, B_Tb0('TIP_OPER', operacion, 'OP2')  operacion, grano producto, ";
                    //command_Fija_Pesifica.CommandText += " substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha , contrato NroContrato, tipoprecio tipoprecio, precio precio, ";
                    //command_Fija_Pesifica.CommandText += " decode(moneda, 'PE', '0', '1') moneda, ttmaxfin TTPactadas, 0 TTentregadas, ittaplicadas TTaplicadas, ";
                    //command_Fija_Pesifica.CommandText += " ittretiro TTretiro, 0 TTfijadas, ittfijadas TTpesificadas, ittliqttotal TTliquidadas,0 TTpendAN ";
                    //command_Fija_Pesifica.CommandText += " FROM acopio.contrato a, mcuit x, mcuenta y ";
                    //command_Fija_Pesifica.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                    //command_Fija_Pesifica.CommandText += " AND x.ctamadre = y.ctamadre ";
                    //command_Fija_Pesifica.CommandText += " AND status = '50' ";
                    //command_Fija_Pesifica.CommandText += " AND fecha > ndate_c('01/01/2018') ";
                    //command_Fija_Pesifica.CommandText += " AND tipoprecio  in ('AF') ";
                    //command_Fija_Pesifica.CommandText += " AND ittaplicadas > ittfijadas ";
                    //command_Fija_Pesifica.CommandText += " AND ittaplicadas > ittliqttotal ";
                    ////command_Fija_Pesifica.CommandText += " AND cosecha > '1718' ";
                    //if (!string.IsNullOrEmpty(cuit))
                    //{
                    //    command_Fija_Pesifica.CommandText += " and (vendcuit = :parametroCUIT)";
                    //    db.AddParameter(command_Fija_Pesifica, "parametroCUIT", cuit.ToString());
                    //}
                    command_Fija_Pesifica.CommandText += " UNION ALL ";
                    command_Fija_Pesifica.CommandText += " SELECT null IDslipAN,'AC' OrigenAdmis, continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, a.vendcta VendCta, compcuit CUITcomp, compnombre, compSTIPPRO, a.compcta CompCta, ";
                    command_Fija_Pesifica.CommandText += " to_char(date_c(fecha), 'dd-mm-yyyy') fecha,a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco, null zona , x.cuit destino, a.destino ctadestino, ";
                    command_Fija_Pesifica.CommandText += " y.nombre nomdestino, y.stipprovee tipodestino, negocio negocio,B_Tb0('TIP_OPER', operacion, 'OP2') operacion, grano producto, ";
                    command_Fija_Pesifica.CommandText += " substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha, contrato NroContrato, tipoprecio tipoprecio,precio precio, ";
                    command_Fija_Pesifica.CommandText += " decode(substr(B_Tb0('MONEDA', a.moneda, 'MN61'), 1, 1), '1', '0', '1') moneda, ttmaxfin TTPactadas, 0 TTentregadas, ittaplicadas TTaplicadas, ";
                    command_Fija_Pesifica.CommandText += " 0 TTretiro, 0 TTfijadas, ittfijadas TTpesificadas, ittliqttotal TTliquidadas, 0 TTpendAN ";
                    command_Fija_Pesifica.CommandText += " FROM corretaje.contrato a, mcuit x, mcuenta y, tmpcuenta cta  ";
                    command_Fija_Pesifica.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                    command_Fija_Pesifica.CommandText += " AND x.ctamadre = y.ctamadre ";
                    command_Fija_Pesifica.CommandText += " AND a.empresa = 1 AND a.status = '50' ";
                    command_Fija_Pesifica.CommandText += " AND a.fecha > ndate_c('01/01/2018') ";
                    command_Fija_Pesifica.CommandText += " AND a.tipoprecio  in ('AF', 'HP') ";
                    command_Fija_Pesifica.CommandText += " AND a.ittaplicadas > a.ittfijadas and a.ittaplicadas > a.ittliqttotal ";
                    command_Fija_Pesifica.CommandText += " AND (a.vendcta = cta.cuenta)";
                    //command_Fija_Pesifica.CommandText += " AND (a.vendcta = cta.cuenta or a.compcta = cta.cuenta)";
                    db.AddParameter(command_Fija_Pesifica, "parametroCUIT", cuit.ToString());                                       

                    //db.AddParameter(command_Fija_Pesifica, "parametroCUIT1", cuit.ToString());
                    //db.AddParameter(command_Fija_Pesifica, "parametroCUIT2", cuit.ToString());
                    //if (!string.IsNullOrEmpty(cuit))
                    //{
                    //    command_Fija_Pesifica.CommandText += " AND (vendcuit = :parametroCUIT1 or compCUIT = :parametroCUIT2)";
                    //    db.AddParameter(command_Fija_Pesifica, "parametroCUIT1", cuit.ToString());
                    //    db.AddParameter(command_Fija_Pesifica, "parametroCUIT2", cuit.ToString());
                    //}
                    //command_Fija_Pesifica.CommandText += " UNION ALL ";
                    //command_Fija_Pesifica.CommandText += " SELECT null IDslipAN, 'AC' OrigenAdmis, continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, compcuit CUITcomp, compnombre, compSTIPPRO, ";
                    //command_Fija_Pesifica.CommandText += " to_char(date_c(fecha), 'dd-mm-yyyy') fecha,a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco, null zona, x.cuit destino, a.destino ctadestino, ";
                    //command_Fija_Pesifica.CommandText += " y.nombre nomdestino, y.stipprovee tipodestino, negocio negocio, B_Tb0('TIP_OPER', operacion, 'OP2') operacion, grano producto, ";
                    //command_Fija_Pesifica.CommandText += " substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha, contrato NroContrato, tipoprecio tipoprecio, precio precio, ";
                    //command_Fija_Pesifica.CommandText += " decode(substr(B_Tb0('MONEDA', a.moneda, 'MN61'), 1, 1), '1', '0', '1') moneda, ttmaxfin TTPactadas,0 TTentregadas, ittaplicadas TTaplicadas, ";
                    //command_Fija_Pesifica.CommandText += " 0 TTretiro, 0 TTfijadas,ittfijadas TTpesificadas, ittliqttotal TTliquidadas, 0 TTpendAN ";
                    //command_Fija_Pesifica.CommandText += " FROM corretaje.contrato a, mcuit x, mcuenta y ";
                    //command_Fija_Pesifica.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                    //command_Fija_Pesifica.CommandText += " AND x.ctamadre = y.ctamadre ";
                    //command_Fija_Pesifica.CommandText += " AND status = '50' "; //  and to_char(date_c(fecha),'yyyy')> '2017' ";
                    //command_Fija_Pesifica.CommandText += " AND fecha > ndate_c('01/01/2018') ";
                    //command_Fija_Pesifica.CommandText += " AND tipoprecio  in ('HP') ";
                    //command_Fija_Pesifica.CommandText += " AND ittaplicadas > ittfijadas and ittaplicadas > ittliqttotal ";
                    ////command_Fija_Pesifica.CommandText += " AND cosecha > '1718' ";
                    //if (!string.IsNullOrEmpty(cuit))
                    //{
                    //    command_Fija_Pesifica.CommandText += " AND (vendcuit = :parametroCUIT1 or compCUIT = :parametroCUIT2)";
                    //    db.AddParameter(command_Fija_Pesifica, "parametroCUIT1", cuit.ToString());
                    //    db.AddParameter(command_Fija_Pesifica, "parametroCUIT2", cuit.ToString());
                    //}
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
        public List<Granario> GetReduccionesContratos(string cuit, string nroCuenta)
        {
            DataSet ds = new DataSet();
            using (DBOracle db = new DBOracle())
            using (OracleConnection conection = db.NewConnection())
            {
                OracleCommand command_Reducciones_Contratos = conection.CreateCommand();
                command_Reducciones_Contratos.CommandType = System.Data.CommandType.Text;
                command_Reducciones_Contratos.BindByName = true;

                string whereNroCuenta = "";
                if (!string.IsNullOrEmpty(nroCuenta))
                    whereNroCuenta= string.Format(" and a.cuenta = {0} ", nroCuenta.ToString());

                command_Reducciones_Contratos.CommandText = string.Format("WITH tmpcuenta AS (SELECT cuenta FROM Mcuenta a INNER JOIN mcuit b ON (a.ctamadre = b.ctamadre AND b.cuit = :parametroCUIT {0}))", whereNroCuenta);
                command_Reducciones_Contratos.CommandText += " SELECT null IDslipAN, 'AC' OrigenAdmis, continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, a.vendcta VendCta, compcuit CUITcomp, compnombre, COMPSTIPPRO, a.compcta CompCta, to_char(date_c(fecha), 'dd-mm-yyyy') fecha, ";
                command_Reducciones_Contratos.CommandText += " a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco, a.zona zona, x.cuit destino, a.destino ctadestino, y.nombre nomdestino, y.stipprovee, negocio negocio, ";
                command_Reducciones_Contratos.CommandText += " B_Tb0('TIP_OPER', operacion, 'OP2')  operacion, grano producto, substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha , contrato NroContrato, tipoprecio tipoprecio, ";
                command_Reducciones_Contratos.CommandText += " precio precio, decode(moneda, 'PE', '0', '1') moneda , ttmaxfin TTPactadas, 0 TTentregadas, ittaplicadas TTaplicadas, ittretiro TTretiro, 0 TTfijadas, ";
                command_Reducciones_Contratos.CommandText += " ittfijadas TTpesificadas, ittliqttotal TTliquidadas, 0 TTpendAN ";
                command_Reducciones_Contratos.CommandText += " FROM acopio.contrato a, mcuit x, mcuenta y, tmpcuenta cta ";
                command_Reducciones_Contratos.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                command_Reducciones_Contratos.CommandText += " AND x.ctamadre = y.ctamadre ";
                command_Reducciones_Contratos.CommandText += " AND a.empresa = 1 AND a.status = '50' ";
                command_Reducciones_Contratos.CommandText += " AND a.fecha > ndate_c('01/01/2018') ";
                command_Reducciones_Contratos.CommandText += " AND a.vendcta = cta.cuenta ";
                ////command_Reducciones_Contratos.CommandText += " AND cosecha > '1718' ";
                //if (!string.IsNullOrEmpty(cuit))
                //{
                //    command_Reducciones_Contratos.CommandText +=  " and vendcuit = :parametroCUIT ";
                //    db.AddParameter(command_Reducciones_Contratos, "parametroCUIT", cuit.ToString());
                //}
                command_Reducciones_Contratos.CommandText += " UNION ALL ";
                command_Reducciones_Contratos.CommandText += " SELECT null IDslipAN , 'AC' OrigenAdmis, continterno continterno, vendcuit CUITvend, vendnombre,VENDSTIPPRO, a.vendcta VendCta, compcuit CUITcomp, compnombre, compSTIPPRO, a.compcta CompCta, to_char(date_c(fecha), 'dd-mm-yyyy') fecha, ";
                command_Reducciones_Contratos.CommandText += " a.uninego uninego, B_Tb0('ZCOMER', a.zona, 'Z8') centro, null zacco , null zona , x.cuit destino, a.destino ctadestino, y.nombre nomdestino, y.stipprovee tipodestino, ";
                command_Reducciones_Contratos.CommandText += " negocio negocio, B_Tb0('TIP_OPER', operacion, 'OP2') operacion, grano producto, substr(cosecha, 1, 2)|| '-' || substr(cosecha, 3, 2) cosecha, contrato NroContrato, ";
                command_Reducciones_Contratos.CommandText += " tipoprecio tipoprecio, precio precio, decode(substr(B_Tb0('MONEDA', a.moneda, 'MN61'), 1, 1), '1', '0', '1') moneda , ttmaxfin TTPactadas, 0 TTentregadas, ittaplicadas TTaplicadas, ";
                command_Reducciones_Contratos.CommandText += " 0 TTretiro, 0 TTfijadas, ittfijadas TTpesificadas, ittliqttotal TTliquidadas, 0 TTpendAN ";
                command_Reducciones_Contratos.CommandText += " FROM corretaje.contrato a, mcuit x, mcuenta y, tmpcuenta cta ";
                command_Reducciones_Contratos.CommandText += " WHERE y.empresa = a.empresa and y.cuenta = a.destino ";
                command_Reducciones_Contratos.CommandText += " AND x.ctamadre = y.ctamadre ";
                command_Reducciones_Contratos.CommandText += " AND a.empresa = 1 AND a.status = '50' ";
                command_Reducciones_Contratos.CommandText += " AND a.fecha > ndate_c('01/01/2018') ";
                command_Reducciones_Contratos.CommandText += " and (a.vendcta = cta.cuenta or a.compcta = cta.cuenta) ";
                db.AddParameter(command_Reducciones_Contratos, "parametroCUIT", cuit.ToString());               
                //command_Reducciones_Contratos.CommandText += " AND cosecha > '1718' ";
                //if (!string.IsNullOrEmpty(cuit))
                //{
                //    command_Reducciones_Contratos.CommandText += " and(vendcuit = :parametroCUIT1 or compCUIT = :parametroCUIT2)";
                //    db.AddParameter(command_Reducciones_Contratos, "parametroCUIT1", cuit.ToString());
                //    db.AddParameter(command_Reducciones_Contratos, "parametroCUIT2", cuit.ToString());
                //}
                using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command_Reducciones_Contratos))
                {
                    dataAdapter.Fill(ds);
                }
                List<Granario> result = new List<Granario>();
                result = ds.Tables[0].ToObject<Granario>().ToList();
                return result;
            }
        }
        public DataSet GetDisponibilidad(string cuit)
        {
            List<DisponibilidadGranaria> result = new List<DisponibilidadGranaria>();
            DataSet ds = new DataSet();
            using (DBOracle db = new DBOracle())
            using (OracleConnection conection = db.NewConnection())
            {
                OracleCommand command_get_disponibilidad = conection.CreateCommand();
                command_get_disponibilidad.CommandType = System.Data.CommandType.Text;
                command_get_disponibilidad.BindByName = true;

                command_get_disponibilidad.CommandText = " SELECT NVL(a.zonastr,' ') as zonastr, ";
                command_get_disponibilidad.CommandText += " DECODE(NVL(Trim(Upper(NVL(b.zona, ' '))), ' '), ' ', 'NO', 'SI') as disponibilidad, ";
                command_get_disponibilidad.CommandText += " DECODE(trim(NVL(a.zonastr, '')), '', 'Vacía', a.zonastr || ' - ') ||  b_TB1('ZCOMER', trim(a.zonastr), 'Z2', 1) as nombrezona, ";
                command_get_disponibilidad.CommandText += " Trim(b.nomgrano) nomgrano, b.cosecha, b.ttentregadas, b.ttretiradas, b.tttransferidas, ";
                command_get_disponibilidad.CommandText += " b.ttcertificadas, b.ttliquidafin, b.parpendiente, b.ttdisponibles FROM acaros.xmaesal a, ";
                //command_get_disponibilidad.CommandText += " (SELECT Trim(Upper(NVL(zona, ' '))) AS zona, Trim(nomgrano) as nomgrano, ";
                //command_get_disponibilidad.CommandText += " cosecha, ttentregadas, ttretiradas, tttransferidas, ";
                //command_get_disponibilidad.CommandText += " ttcertificadas, ttliquidafin, parpendiente, ttdisponibles FROM acaros.vtotalzonagrano ";
                //command_get_disponibilidad.CommandText += " WHERE ttdisponibles <> 0 ) b, ";
                command_get_disponibilidad.CommandText += " acaros.vtotalzonagrano b, acaros.xmaestro c, acopio.mcuenta d, acopio.mcuit e ";
                command_get_disponibilidad.CommandText += " WHERE e.cuit= :param ";
                command_get_disponibilidad.CommandText += " AND NVL(d.ctaint,0) <> 0 AND a.cuenta = c.cuenta AND a.cuenta = TO_CHAR(d.ctaint(+)) ";
                command_get_disponibilidad.CommandText += " AND Trim(Upper(Nvl(a.zonastr,' '))) = b.zona(+) ";
                command_get_disponibilidad.CommandText += " AND d.ctamadre = e.ctamadre ";
                command_get_disponibilidad.CommandText += " GROUP BY a.zonastr, b.zona, b.nomgrano, b.cosecha, b.ttentregadas, b.ttretiradas, b.tttransferidas, b.ttcertificadas, b.ttliquidafin,b.parpendiente, b.ttdisponibles, c.tipprovee ";
                command_get_disponibilidad.CommandText += " HAVING b.ttdisponibles <> 0 AND c.tipprovee IN('0000','0051', '0052', '0061')  ";
                command_get_disponibilidad.CommandText += " ORDER BY zonastr, b.nomgrano, b.cosecha ";

                //command_get_disponibilidad.CommandText += " SELECT DECODE(NVL(b.zona, ' '), ' ', 'NO', 'SI') as disponibilidad, from ";
                //command_get_disponibilidad.CommandText += " (SELECT Trim(Upper(NVL(zona, ' '))) AS zona, Trim(nomgrano) as nomgrano, ";
                //command_get_disponibilidad.CommandText += " cosecha, ttentregadas, ttretiradas, tttransferidas, ";
                //command_get_disponibilidad.CommandText += " ttcertificadas, ttliquidafin, parpendiente, ttdisponibles FROM acaros.vtotalzonagrano ";
                //command_get_disponibilidad.CommandText += " WHERE ttdisponibles <> 0 ) b  ";
                db.AddParameter(command_get_disponibilidad, "param", cuit.ToString());
             
                using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command_get_disponibilidad))
                {
                    dataAdapter.Fill(ds);
                    return ds;
                }

                //if (ds.Tables.Count > 0)
                //{
                //    result = ds.Tables[0].ToObject<DisponibilidadGranaria>().ToList();
                //}

            }
        }
        public List<SaldoGranario> GetSaldoGranario(string cuit)
        {
            List<SaldoGranario> result = new List<SaldoGranario>();
            
            DataSet ds = new DataSet();
            using (DBOracle db = new DBOracle())
            using (OracleConnection conection = db.NewConnection())
            {
                OracleCommand command_get_disponibilidad = conection.CreateCommand();
                command_get_disponibilidad.CommandType = System.Data.CommandType.Text;
                command_get_disponibilidad.BindByName = true;

                command_get_disponibilidad.CommandText = " SELECT    bb.cuit, " +
                                                           " SUM(NVL(a.saldo,0)) as cta,  " +
                                                           " SUM(NVL(a.pes_d, 0) - NVL(a.pes_h, 0)) as difer_pesos, " +
                                                           " SUM(NVL(a.dol_d, 0) - NVL(a.dol_h, 0)) as difer_dolares, " +
                                                           " SUM(NVL(a.canje, 0) + NVL(a.termi, 0) + NVL(a.anticip, 0)) as varios, " +
                                                           " SUM(NVL(a.saldo, 0) + (NVL(a.pes_d, 0) - NVL(a.pes_h, 0)) + " +
                                                           " (NVL(a.dol_d, 0) - NVL(a.dol_h, 0)) + " +
                                                           " (NVL(a.canje, 0) + NVL(a.termi, 0) + NVL(a.anticip, 0))) as saldo " +
                                                           " FROM acaros.xmaesal a, acaros.xmaestro c, acopio.mcuenta d, acopio.mcuit bb " +
                                                           " WHERE a.cuenta = c.cuenta " +
                                                            " AND a.cuenta = TO_CHAR(d.ctaint(+)) " +
                                                            " AND NVL(d.ctaint,0) <> 0 " +
                                                            " AND bb.ctamadre = d.ctamadre " +
                                                            " AND  bb.cuit = :param " +
                                                            " GROUP BY  bb.cuit,c.tipprovee " +
                                                            " HAVING   c.tipprovee IN('0000','0051', '0052', '0061') ";
                                                            
                 db.AddParameter(command_get_disponibilidad, "param", cuit.ToString());

                using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command_get_disponibilidad))
                {
                    dataAdapter.Fill(ds);
                }

                if (ds.Tables.Count > 0)
                {
                    result = ds.Tables[0].ToObject<SaldoGranario>().ToList();
                }
                return result;
            }
        }
    }
}