from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when, current_date

def run_etl():
    # 1. Spark Oturumunu Başlat
    spark = SparkSession.builder \
        .appName("ECommerce_Sales_ETL") \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()

    print("Spark oturumu başlatıldı. Veriler okunuyor...")

    # 2. Extract (Veriyi Çekme - Örnek bir CSV'den okunduğu varsayılır)
    # df = spark.read.csv("raw_data/sales_data.csv", header=True, inferSchema=True)
    
    # Portfolyo için örnek veri oluşturma (Bakan kişi kodun mantığını görsün diye)
    mock_data = [
        ("TR-101", "Elektronik", 4500.50, "Erzurum", None),
        ("TR-102", "Giyim", 300.00, "Istanbul", "Kredi Karti"),
        ("TR-103", "Elektronik", None, "Ankara", "Nakit"),
        ("TR-104", "Gıda", 150.75, "Erzurum", "Kredi Karti")
    ]
    columns = ["Siparis_ID", "Kategori", "Tutar", "Sehir", "Odeme_Tipi"]
    df = spark.createDataFrame(mock_data, columns)

    # 3. Transform (Veri Temizleme ve Dönüştürme)
    # - Tutar'ı boş olan satırları sil
    # - "Durum" adında yeni bir kolon ekle (Tutara göre sınıflandır)
    clean_df = df.dropna(subset=["Tutar"]) \
                 .fillna({"Odeme_Tipi": "Bilinmiyor"}) \
                 .withColumn("Durum", when(col("Tutar") > 1000, "Yüksek İşlem").otherwise("Normal İşlem")) \
                 .withColumn("Tutar", round(col("Tutar"), 2))

    print("Veri dönüştürme işlemi tamamlandı. Özet tablo:")
    
    # Şehirlere göre toplam satış analizi
    summary_df = clean_df.groupBy("Sehir").sum("Tutar").withColumnRenamed("sum(Tutar)", "Toplam_Satis")
    summary_df.show()

    # 4. Load (Veriyi Yükleme)
    # Temizlenmiş veriyi Parquet formatında (Büyük veri standardı) kaydet
    # clean_df.write.mode("overwrite").parquet("processed_data/clean_sales.parquet")
    
    print("ETL süreci başarıyla tamamlandı.")
    spark.stop()

if __name__ == "__main__":
    run_etl()
