package minsait.ttaa.datio;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.FileSystemResource;

import static minsait.ttaa.datio.common.Common.PARAMS_PATH;

@Configuration
public class PropertiesConfiguration {
    @Bean
    public PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        PropertySourcesPlaceholderConfigurer properties =
                new PropertySourcesPlaceholderConfigurer();
        properties.setLocation(new FileSystemResource(PARAMS_PATH));
        properties.setIgnoreResourceNotFound(false);
        return properties;
    }
}
