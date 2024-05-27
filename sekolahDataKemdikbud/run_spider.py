import subprocess

# Daftar nama spider
spiders = [
    'spider1',
    'spider2',
    'spider3',
    'spider4',
    'spider5',
    'spider6',
    'spider7',
    'spider8',
    'spider9',
    'spider10',
    'spider11',
    'spider12',
    'spider13',
    'spider14',
    'spider15',
    'spider16',
    'spider17',
    'spider18',
    'spider19',
    'spider20',
    'spider21',
    'spider22',
    'spider23',
    'spider24',
    'spider25',
    'spider26',
    'spider27',
    ]

def run_spider(spider_name):
    # Jalankan spider menggunakan subprocess
    subprocess.run(['scrapy', 'crawl', spider_name])

def main():
    # Jalankan setiap spider secara bergantian
    for spider in spiders:
        print(f"Running spider: {spider}")
        run_spider(spider)
        print(f"Spider {spider} finished\n")

if __name__ == "__main__":
    main()
