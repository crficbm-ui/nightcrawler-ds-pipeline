import streamlit as st

def display_dict(dict_obj, name="Dictionary"):
    with st.expander(name):
        st.write(dict_obj)

############################################
# (1) SERP Logic
############################################
from nightcrawler.logic.s01_serp import SerpLogic

serp_logic = SerpLogic()

def view_serp_logic(keyword, number_of_results):
    with st.spinner("Running Serp Logic..."):
        serp_items = serp_logic.apply_one(keyword=keyword, number_of_results=number_of_results, full_output=True)
        display_dict(serp_items, "SERP Items")

    return serp_items

############################################
# (2) Zyte Logic
############################################
from nightcrawler.logic.s01_zyte import ZyteLogic

zyte_config = {
    "geolocation": "CH",
    "product": True,
    "productOptions": {"extractFrom": "httpResponseBody"},
    "httpResponseBody": True
}
zyte_api_config = {
    "max_retries": 1
}

zyte_logic = ZyteLogic(config=zyte_config, api_config=zyte_api_config)

def view_zyte_logic(url, prefix=""):
    with st.spinner("Running Zyte Logic..."):
        try:
            zyte_output = zyte_logic.apply_one({"url": url})
        except ValueError as e:
            st.write("Failed to collect product from", url)
            st.write(e)
            return None

    st.write(f"{prefix} {zyte_output['title']}")
    display_dict(zyte_output, "Zyte Output")
    
    return zyte_output

############################################
# (3) Delivery Policy Detection Logic
############################################

# CODE HERE

############################################
# (4) Page Type Detection Logic
############################################
from nightcrawler.logic.s03_page_type_detection import PageTypeDetectionZyteLogic, PageTypes

page_type_detection_logic = PageTypeDetectionZyteLogic()

def view_page_type_detection_logic(zyte_output):
    with st.spinner("Running Page Type Detection Logic..."):
        page_type_detection_output = page_type_detection_logic.apply_one({"zyte_probability": zyte_output["zyte_probability"]})
        display_dict(page_type_detection_output, "Page Type Detection Output")

    if page_type_detection_output["page_type"] == PageTypes.ECOMMERCE_PRODUCT:
        col1, col2 = st.columns(2)
        with col2:
            st.write(zyte_output["title"])
            st.write(zyte_output["price"])
            st.write(zyte_output["zyte_probability"])
            st.write(zyte_output["full_description"])

    return page_type_detection_output

############################################
# (5) Product Domain Detection Logic
################################

# CODE HERE


############################################
# Streamlit App
############################################

def main():
    st.write("# Nightcrawler Demo")

    # (0) Inputs
    keyword = st.text_input("Keyword", "buy aspirin online")
    number_of_results = st.slider("Number of Results", 1, 100, 20)

    if st.button("Run"):
        # (1) SERP Logic
        serp_items = view_serp_logic(keyword, number_of_results)

        for i, serp_item in enumerate(serp_items):
            url = serp_item.get("link")
            
            # (2) Zyte Logic
            zyte_output = view_zyte_logic(url, prefix=f"{i+1}. ")
            if not zyte_output:
                continue

            # (3) Delivery Policy Detection Logic
            # CODE HERE

            # (4) Page Type Detection Logic
            page_type_detection_output = view_page_type_detection_logic(zyte_output)

            # (5) Product Domain Detection Logic
            # CODE HERE

if __name__ == "__main__":
    main()