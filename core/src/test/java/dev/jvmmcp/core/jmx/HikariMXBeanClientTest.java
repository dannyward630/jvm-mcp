package dev.jvmmcp.core.jmx;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.jvmmcp.core.model.HikariPoolStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HikariMXBeanClientTest {

    private JmxConnectionManager connectionManager;
    private HikariDataSource dataSource;
    private HikariDataSource secondDataSource;
    private HikariMXBeanClient hikariClient;

    @Mock
    private MBeanServerConnection mockMbsc;

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setPoolName("TestHikariPool");
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setJdbcUrl("jdbc:h2:mem:test1;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        config.setRegisterMbeans(true);

        dataSource = new HikariDataSource(config);

        HikariConfig secondConfig = new HikariConfig();
        secondConfig.setPoolName("SecondTestHikariPool");
        secondConfig.setMaximumPoolSize(10);
        secondConfig.setMinimumIdle(1);
        secondConfig.setJdbcUrl("jdbc:h2:mem:test2;DB_CLOSE_DELAY=-1");
        secondConfig.setUsername("sa");
        secondConfig.setPassword("");
        secondConfig.setRegisterMbeans(true);

        secondDataSource = new HikariDataSource(secondConfig);

        connectionManager = JmxConnectionManager.connectLocal();
        hikariClient = new HikariMXBeanClient(connectionManager.getMBeanServerConnection());
    }

    @AfterEach
    void tearDown() {
        if (secondDataSource != null) {
            secondDataSource.close();
        }
        if (dataSource != null) {
            dataSource.close();
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
    }

    @Test
    @DisplayName("Constructor throws IllegalArgumentException when MBeanServerConnection is null")
    void shouldThrowWhenMbscIsNull() {
        assertThatThrownBy(() -> new HikariMXBeanClient(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("MBeanServerConnection cannot be null");
    }

    @Test
    @DisplayName("getPools discovers all registered HikariCP pools")
    void shouldDiscoverHikariPools() throws IOException {
        List<HikariPoolStatistics> pools = hikariClient.getPools();

        assertThat(pools)
            .extracting(HikariPoolStatistics::poolName)
            .contains("TestHikariPool", "SecondTestHikariPool");
    }

    @Test
    @DisplayName("getPools discovers standard comma-separated HikariCP pool names")
    void shouldDiscoverStandardHikariPoolNames() throws Exception {
        ObjectName poolName = new ObjectName("com.zaxxer.hikari:type=Pool,name=StandardPool");
        ObjectName configName = new ObjectName("com.zaxxer.hikari:type=PoolConfig,name=StandardPool");
        when(mockMbsc.queryNames(any(), any())).thenReturn(Set.of(), Set.of(poolName));
        when(mockMbsc.getAttribute(poolName, "ActiveConnections")).thenReturn(1);
        when(mockMbsc.getAttribute(poolName, "IdleConnections")).thenReturn(2);
        when(mockMbsc.getAttribute(poolName, "TotalConnections")).thenReturn(3);
        when(mockMbsc.getAttribute(poolName, "ThreadsAwaitingConnection")).thenReturn(0);
        when(mockMbsc.getAttribute(configName, "MaximumPoolSize")).thenReturn(5);

        try (HikariMXBeanClient client = new HikariMXBeanClient(mockMbsc)) {
            List<HikariPoolStatistics> pools = client.getPools();

            assertThat(pools).hasSize(1);
            assertThat(pools.get(0).poolName()).isEqualTo("StandardPool");
            assertThat(pools.get(0).maximumPoolSize()).isEqualTo(5);
        }

        ArgumentCaptor<ObjectName> patternCaptor = ArgumentCaptor.forClass(ObjectName.class);
        verify(mockMbsc, times(2)).queryNames(patternCaptor.capture(), any());
        assertThat(patternCaptor.getAllValues())
            .containsExactly(
                new ObjectName("com.zaxxer.hikari:type=Pool (*)"),
                new ObjectName("com.zaxxer.hikari:type=Pool,*")
            );
    }

    @Test
    @DisplayName("readPool extracts connection counts and pool metrics")
    void shouldReadPoolStatistics() throws IOException {
        List<HikariPoolStatistics> pools = hikariClient.getPools();

        HikariPoolStatistics pool = pools.stream()
            .filter(p -> p.poolName().equals("TestHikariPool"))
            .findFirst()
            .orElseThrow();

        assertThat(pool.activeConnections()).isGreaterThanOrEqualTo(0);
        assertThat(pool.idleConnections()).isGreaterThanOrEqualTo(0);
        assertThat(pool.totalConnections()).isGreaterThanOrEqualTo(0);
        assertThat(pool.threadsAwaitingConnection()).isGreaterThanOrEqualTo(0);
        assertThat(pool.maximumPoolSize()).isEqualTo(5);
        assertThat(pool.saturationRatio()).isBetween(0.0, 1.0);
    }

    @Test
    @DisplayName("getPools wraps MBeanServer query exceptions into IOException")
    void shouldWrapQueryExceptions() throws Exception {
        when(mockMbsc.queryNames(any(), any())).thenThrow(new RuntimeException("JMX query failure"));

        try (HikariMXBeanClient client = new HikariMXBeanClient(mockMbsc)) {
            assertThatThrownBy(client::getPools)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to query HikariCP MBeans");
        }
    }

    @Test
    @DisplayName("readPool wraps attribute read exceptions into IOException")
    void shouldWrapReadPoolExceptions() throws Exception {
        ObjectName poolName = new ObjectName("com.zaxxer.hikari:type=Pool (FailingPool)");
        when(mockMbsc.getAttribute(poolName, "ActiveConnections")).thenThrow(new RuntimeException("Attr read failure"));

        try (HikariMXBeanClient client = new HikariMXBeanClient(mockMbsc)) {
            assertThatThrownBy(() -> client.readPool(poolName))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to read HikariCP pool");
        }
    }

    @Test
    @DisplayName("getMaximumPoolSize wraps attribute read exceptions into IOException")
    void shouldWrapGetMaximumPoolSizeExceptions() throws Exception {
        ObjectName configName = new ObjectName("com.zaxxer.hikari:type=PoolConfig (FailingPool)");
        when(mockMbsc.getAttribute(configName, "MaximumPoolSize")).thenThrow(new RuntimeException("Config read failure"));

        try (HikariMXBeanClient client = new HikariMXBeanClient(mockMbsc)) {
            assertThatThrownBy(() -> client.getMaximumPoolSize(configName))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to read maximum pool size");
        }
    }

    @Test
    @DisplayName("extractPoolName strips prefix/suffix or returns raw name when not formatted")
    void shouldExtractPoolNames() throws Exception {
        assertThat(hikariClient.extractPoolName(new ObjectName("com.zaxxer.hikari:type=Pool (Custom)")))
            .isEqualTo("Custom");
        assertThat(hikariClient.extractPoolName(new ObjectName("com.zaxxer.hikari:type=OtherPool")))
            .isEqualTo("com.zaxxer.hikari:type=OtherPool");
    }

    @Test
    @DisplayName("readPool handles custom pool names and zero maximumPoolSize")
    void shouldHandleCustomObjectNamesAndZeroPoolSize() throws Exception {
        ObjectName customName = new ObjectName("com.zaxxer.hikari:type=Pool (CustomPool)");
        when(mockMbsc.queryNames(any(), any())).thenReturn(Set.of(customName));
        when(mockMbsc.getAttribute(customName, "ActiveConnections")).thenReturn(0);
        when(mockMbsc.getAttribute(customName, "IdleConnections")).thenReturn(0);
        when(mockMbsc.getAttribute(customName, "TotalConnections")).thenReturn(0);
        when(mockMbsc.getAttribute(customName, "ThreadsAwaitingConnection")).thenReturn(0);

        ObjectName configName = new ObjectName("com.zaxxer.hikari:type=PoolConfig (CustomPool)");
        when(mockMbsc.getAttribute(configName, "MaximumPoolSize")).thenReturn(0);

        try (HikariMXBeanClient client = new HikariMXBeanClient(mockMbsc)) {
            List<HikariPoolStatistics> pools = client.getPools();

            assertThat(pools).hasSize(1);
            assertThat(pools.get(0).poolName()).isEqualTo("CustomPool");
            assertThat(pools.get(0).saturationRatio()).isEqualTo(-1.0);
        }
    }
}




