using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tipos
{
    public class Granario
    {
        public long IDslipAN { get; set; }
        public string OrigenAdmis { get; set; }
        public long ContInterno { get; set; }
        public string CUITVend { get; set; }
        public string VendNombre { get; set; }
        public string VendStipPro { get; set; }
        public string VendCta { get; set; }
        public string CUITComp { get; set; }
        public string CompNombre { get; set; }
        public string CompStipPro { get; set; }
        public string CompCta { get; set; }
        public string Fecha { get; set; }
        public string UniNego { get; set; }
        public string Centro { get; set; }
        public string Zacco { get; set; }
        public string Zona { get; set; }
        public string Destino { get; set; }
        public string CtaDestino { get; set; }
        public string NomDestino { get; set; }
        public int StipProvee { get; set; }
        public string Negocio { get; set; }
        public string Operacion { get; set; }
        public string Producto { get; set; }
        public string Cosecha { get; set; }
        public string NroContrato { get; set; }
        public string TipoPrecio { get; set; }
        public double Precio { get; set; }
        public string Moneda { get; set; }
        public double TTPactadas { get; set; }
        public double TTEntregadas { get; set; }
        public double TTAplicadas { get; set; }
        public double TTRetiro { get; set; }
        public double TTFijadas { get; set; }
        public double TTPesificadas { get; set; }
        public double TTLiquidadas { get; set; }
        public double TTPendAn { get; set; } 
    }
}
