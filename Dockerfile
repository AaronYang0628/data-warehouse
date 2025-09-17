# 使用包含Maven和Java 17的镜像作为构建环境
FROM m.daocloud.io/docker.io/library/maven:3.9-eclipse-temurin-17 AS builder

# 设置工作目录
WORKDIR /app

# 复制pom.xml和源代码
COPY pom.xml .
COPY src ./src

# 使用镜像中预装的Maven进行构建（不再需要mvnw）
RUN mvn clean package -DskipTests

# 创建运行时镜像
FROM m.daocloud.io/docker.io/library/eclipse-temurin:17-jre

WORKDIR /app

# 从构建阶段复制构建好的jar包
COPY --from=builder /app/target/metadata-operator-0.1.0-SNAPSHOT.jar ./app.jar

# 设置入口点
ENTRYPOINT ["java", "-jar", "app.jar"]