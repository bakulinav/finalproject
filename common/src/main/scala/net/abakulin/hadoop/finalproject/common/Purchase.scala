package net.abakulin.hadoop.finalproject.common

case class Purchase(product: Product, price: Double, date: String, clientIp: String){

    override def toString: String = f"${product.name},$price%.2f,$date,${product.category},$clientIp"
}