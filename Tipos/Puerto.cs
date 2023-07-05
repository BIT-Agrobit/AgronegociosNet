using System;

namespace Models
{
    public class Puerto
    {
        public virtual long Id { get; set; }
        public virtual long Cuenta { get; set; }
        public virtual string Cuit { get; set; }
        public virtual string Domicilio { get; set; }
        public virtual string Localidad { get; set; }
        public virtual string Nombre { get; set; }
        public virtual string Provincia { get; set; }
        public virtual int Stipprovee { get; set; }
        public virtual string Tipodecuenta { get; set; }
        public virtual long Cpostal { get; set; }
        public virtual string Ruca { get; set; }
        public virtual string IdTerminal { get; set; }

        public virtual bool Equals(Object obj)
        {
            //Check for null and compare run-time types.
            if ((obj == null) || !this.GetType().Equals(obj.GetType()))
            {
                return false;
            }
            else
            {
                Puerto c = (Puerto)obj;
                return this.Id == c.Id;
            }
        }

        public virtual int GetHashCode()
        {
            return this.Id.GetHashCode();
        }
    }
}