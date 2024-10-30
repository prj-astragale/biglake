FROM python:3.10.12-slim-bookworm
# ENV PYTHONPATH "${PYTHONPATH}:/app/"

WORKDIR /code
# open3d needs libegl1 & libgl1 & libgomp1 cf http://www.open3d.org/docs/release/docker.html
RUN apt-get update && apt-get install --no-install-recommends -y \
    libegl1 \
    libgl1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# open3dwheel
# COPY ./_tmp/open3d-0.17.0-cp310-cp310-manylinux_2_27_x86_64.whl ./open3d-0.17.0-cp310-cp310-manylinux_2_27_x86_64.whl
# RUN pip install open3d-0.17.0-cp310-cp310-manylinux_2_27_x86_64.whl
# RUN rm ./open3d-0.17.0-cp310-cp310-manylinux_2_27_x86_64.whl

COPY ./requirements.txt /code/requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt



COPY .env /code/.env
COPY ./app /code/app
# COPY ./fastfiles /code/fastfiles
COPY ./fast_clients /code/fast_clients 
COPY ./processors /code/processors

RUN mkdir /code/uploads
RUN mkdir /uploads

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]