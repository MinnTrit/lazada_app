from flask import render_template, request
from tasks import db, app
from sqlalchemy import func
from tasks import scrape_lazada, renew_cookies

class Product(db.Model):
    __tablename__ = 'ecommerce_sku' 
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    category_raw = db.Column(db.String(200), nullable=True)
    category_raw_id = db.Column(db.String(200), nullable=True)
    brand_raw = db.Column(db.String(255), nullable=True)
    spu_used_id = db.Column(db.Text, nullable=False)
    spu_id_marketplace = db.Column(db.String(200), nullable=True)
    spu_id_marketplace_seller = db.Column(db.Text, nullable=True)
    used_id = db.Column(db.Text, nullable=False)
    sku_id_marketplace = db.Column(db.Text, nullable=True)
    sku_id_marketplace_seller = db.Column(db.String(500), nullable=True)
    marketplace_code = db.Column(db.String(500), nullable=True)
    barcode = db.Column(db.String(500), nullable=True)
    name = db.Column(db.String(2000), nullable=False)
    variation_name = db.Column(db.String(500), nullable=True)
    url = db.Column(db.String(500), nullable=True)
    img_url = db.Column(db.String(500), nullable=True)
    old_content = db.Column(db.Text, nullable=True)
    status_marketplace = db.Column(db.String(500), nullable=True)
    retail_price = db.Column(db.Numeric(10, 2), nullable=False)
    selling_price = db.Column(db.Numeric(10, 2), nullable=False)
    marketplace_created = db.Column(db.TIMESTAMP, nullable=True)
    marketplace_updated = db.Column(db.TIMESTAMP, nullable=True)
    is_hard_bundle_auto = db.Column(db.Boolean, nullable=True, default=False)
    has_gift_auto = db.Column(db.Boolean, nullable=True, default=False)
    created = db.Column(db.TIMESTAMP, nullable=False)
    updated = db.Column(db.TIMESTAMP, nullable=False)
    source = db.Column(db.String(500), nullable=False)
    fk_brand_id = db.Column(db.Integer, nullable=True)  
    fk_category_id = db.Column(db.Integer, nullable=True)  
    fk_seller_id = db.Column(db.Integer, db.ForeignKey('ecommerce_seller.id'), nullable=False)  
    content_imgs = db.Column(db.Text, nullable=True)
    content_videos = db.Column(db.Text, nullable=True)
    highlights = db.Column(db.Text, nullable=True)
    imgs = db.Column(db.Text, nullable=True)
    content = db.Column(db.Text, nullable=True)
    videos = db.Column(db.Text, nullable=True)

    # Impose the backward references relationships
    seller = db.relationship('EcommerceSeller', backref='skus', lazy=True)

class EcommerceSeller(db.Model):
    __tablename__ = 'ecommerce_seller' 
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    used_id = db.Column(db.String(50), nullable=False)
    id_marketplace = db.Column(db.String(50), nullable=False)
    seller_center_code = db.Column(db.String(255), nullable=True, default=None)
    name = db.Column(db.String(50), nullable=False)
    slug = db.Column(db.String(255), nullable=True, default=None)
    url = db.Column(db.String(500), nullable=True)
    seller_type = db.Column(db.String(255), nullable=True, default='retail')
    token_refresh_latest = db.Column(db.TIMESTAMP, nullable=True)
    source = db.Column(db.String(255), nullable=True, default='lazada_upload')
    created = db.Column(db.TIMESTAMP, nullable=False)
    updated = db.Column(db.TIMESTAMP, nullable=False)
    
    # Foreign keys
    fk_country_id = db.Column(db.Integer, db.ForeignKey('user_management_country.id'), nullable=False)  
    fk_marketplace_id = db.Column(db.Integer, db.ForeignKey('user_management_marketplace.id'), nullable=False) 
    
    # Impose the back references relationships
    country = db.relationship('UserManagementCountry', backref='sellers', lazy=True)  
    marketplace = db.relationship('UserManagementMarketplace', backref='sellers', lazy=True) 

class UserManagementCountry(db.Model):
    __tablename__ = 'user_management_country' 
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    used_id = db.Column(db.String(2), nullable=False)
    name = db.Column(db.String(50), nullable=False)
    currency = db.Column(db.String(3), nullable=False)
    from_usd_xrate = db.Column(db.Float, nullable=False)
    vat = db.Column(db.Float, nullable=False)
    timezone = db.Column(db.String(50), nullable=False)
    updated = db.Column(db.TIMESTAMP, nullable=False)


class UserManagementMarketplace(db.Model):
    __tablename__ = 'user_management_marketplace'  
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    used_id = db.Column(db.String(6), nullable=False)
    name = db.Column(db.String(50), nullable=False)
    background_color = db.Column(db.String(26), nullable=False)
    color = db.Column(db.String(26), nullable=False)
    domain = db.Column(db.String(10), nullable=False)
    allows_product_content_html = db.Column(db.Boolean, nullable=False)
    allows_product_highlights = db.Column(db.Boolean, nullable=False)
    allows_product_video = db.Column(db.Boolean, nullable=False)

@app.route("/", methods=['GET', 'POST'])
def home():
    if request.method == 'GET':
        return render_template('index.html')
    else:
        scrapping_keyword = request.form.get("scrappingKeyword")
        scrapping_pages = int(request.form.get('scrappingPages'))
        scrape_lazada.delay(scrapping_keyword, scrapping_pages)
        return render_template('index.html', message="Jobs launched sucessfully")
    
@app.route("/renew-cookies", methods=['POST'])
def refresh_cookies():
    raw_cookies_string = request.form.get('rawCookies')
    renew_cookies.delay(raw_cookies_string)
    return render_template('index.html', message="Cookies have been renewed")

@app.route("/product-list")
def display_products():
    #Get the product results
    product_results = db.session.query(
        Product.category_raw.label('search_word'),
        Product.name.label('product_name'),
        Product.url.label('product_url'),
        Product.img_url.label('image_url'),
        func.round(Product.old_content, 2).label('product_rating_score'),
        Product.retail_price,
        Product.selling_price,
        Product.content_imgs.label('total_units_sold'),
        Product.content_videos.label('display_price'),
        Product.highlights.label('product_reviews')
    ).all()
    products = [
        {
            "search_word": result.search_word,
            "product_name": result.product_name,
            "product_url": result.product_url,
            "image_url": result.image_url,
            "product_rating_score": result.product_rating_score,
            "retail_price": result.retail_price,
            "selling_price": result.selling_price,
            "total_units_sold": result.total_units_sold,
            "display_price": result.display_price,
            "product_reviews": result.product_reviews,
        }
        for result in product_results
    ]

    #Get the category raw
    category_results = db.session.query(
        Product.category_raw
    ).distinct().all()
    categories = [{'category_raw': orm_object.category_raw} for orm_object in category_results]
    return render_template('display_products.html', products=products, categories=categories)


if __name__ == "__main__":
    app.run(port=5002, debug=False, host='0.0.0.0')
