from PIL import Image

im = Image.open("/Users/davgutavi/Desktop/RE__Envio_de_imagens_nos_diferentes_formatos_/MIlho 2 Out 2018.png", "r")
pix_val = list(im.getdata())
pix_val_flat = [x for sets in pix_val for x in sets]


print(pix_val_flat)