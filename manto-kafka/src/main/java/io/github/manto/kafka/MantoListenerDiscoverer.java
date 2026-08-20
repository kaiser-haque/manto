package io.github.manto.kafka;

import io.github.manto.core.MantoListener;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ListableBeanFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Discovers {@link MantoListener}-annotated methods on application beans.
 *
 * <p>Scanning happens on demand (typically during listener registration,
 * after all singletons are instantiated) so every bean is available.
 * Proxied beans are unwrapped to their target class before reflection.</p>
 */
public class MantoListenerDiscoverer {

    private final MantoListenerValidator validator;

    public MantoListenerDiscoverer(MantoListenerValidator validator) {
        this.validator = validator;
    }

    /**
     * Scans the bean factory for {@link MantoListener} handlers.
     *
     * @param beanFactory the factory holding the application beans
     * @return the discovered listener definitions, in bean definition order
     * @throws MantoListenerConfigurationException if a listener method is invalid
     */
    public List<MantoListenerDefinition> discover(ListableBeanFactory beanFactory) {
        Map<String, Object> beans = new LinkedHashMap<>();
        for (String beanName : beanFactory.getBeanDefinitionNames()) {
            Object bean = beanFactory.getBean(beanName);
            if (bean != null) {
                beans.put(beanName, bean);
            }
        }

        List<MantoListenerDefinition> definitions = new ArrayList<>();
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            Object bean = entry.getValue();
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            for (Method method : targetClass.getMethods()) {
                MantoListener annotation = method.getAnnotation(MantoListener.class);
                if (annotation != null) {
                    definitions.add(toDefinition(bean, method, annotation));
                }
            }
        }
        return definitions;
    }

    private MantoListenerDefinition toDefinition(Object bean, Method method, MantoListener annotation) {
        String topic = annotation.topic();
        String groupId = annotation.groupId();
        validator.validate(method, topic, groupId);
        return new MantoListenerDefinition(bean, method, topic, groupId);
    }
}